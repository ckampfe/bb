use crate::metainfo::MetaInfo;
use crate::torrent::{self, GossipMessage, PeerArgs, Pieces};
use crate::{download, timeout, InfoHash, PeerId};
use bitvec::{bitvec, order::Msb0};
use futures::sink::SinkExt;
use protocol::{Frame, HandshakeFrame, PeerProtocol};
use std::collections::VecDeque;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{broadcast, mpsc, RwLock, Semaphore, TryAcquireError};
use tokio::time::interval;
use tokio::{select, task::JoinHandle, time::error::Elapsed};
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, FramedWrite};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, instrument};

mod protocol;

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error("couldn't send control message")]
    Control(#[from] tokio::sync::mpsc::error::SendError<TorrentToPeer>),
    #[error("timed out")]
    Timeout(#[from] Elapsed),
    #[error("io")]
    Io(#[from] std::io::Error),
    #[error("connection closed")]
    ConnectionClosed,
    #[error("handshake")]
    Handshake(&'static str),
    #[error("protocol error")]
    Protocol(#[from] protocol::Error),
    #[error("no more available connections")]
    NoAvailableConnections(#[from] TryAcquireError),
    #[error("no metainfo available")]
    Metainfo,
    #[error("unable to broadcast to other peers")]
    P2PBroadcast(#[from] tokio::sync::broadcast::error::SendError<GossipMessage>),
    #[error("unable to get pieces for this torrent")]
    NoPieces,
}

pub enum TorrentToPeer {
    Unchoke,
    Shutdown,
}

#[derive(Debug, PartialEq)]
struct Request {
    index: u32,
    begin: u32,
    length: u32,
}

impl From<Request> for protocol::Frame {
    fn from(value: Request) -> Self {
        Frame::Request {
            index: value.index,
            begin: value.begin,
            length: value.length,
        }
    }
}

/// just a dumb little thing that allows us
/// to queue up a batch of requests so that:
/// 1. we don't make only one download request at a time
/// 2. we can implement cancel, as the requests are reified objects
#[derive(Debug)]
struct AsyncQueue<T> {
    capacity: usize,
    q: VecDeque<T>,
}

impl<T: PartialEq> AsyncQueue<T> {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            q: VecDeque::with_capacity(capacity),
        }
    }

    /// either enqueues the item or returns immediately
    /// if there is no remaining capacity.
    /// returns `true` if the item was enqueued, `false` if not.
    fn try_push_back(&mut self, item: T) -> bool {
        if self.q.len() < self.capacity {
            self.q.push_back(item);
            true
        } else {
            false
        }
    }

    /// either dequeues an item or returns immediately
    /// if there is no item
    async fn try_pop_front(&mut self) -> Option<T> {
        self.q.pop_front()
    }

    /// remove all items from the queue
    fn clear(&mut self) {
        self.q.clear();
    }

    /// keep items for which `predicate` returns true
    fn filter<P: FnMut(&T) -> bool>(&mut self, predicate: P) {
        let q_moved_out = std::mem::take(&mut self.q);
        self.q = q_moved_out.into_iter().filter(predicate).collect();
    }
}

// TODO implement Debug manually for this, since it contains undebuggable fields
struct State {
    info_hash: InfoHash,
    metainfo: Arc<MetaInfo>,
    /// used to communicate status information to other peer tasks
    gossip_tx: broadcast::Sender<GossipMessage>,
    /// where the downloaded data lives
    // TODO make this updatable by the torrent task
    data_path: PathBuf,
    /// me
    my_id: PeerId,
    /// send messages to the owning torrent
    torrent_tx: mpsc::Sender<torrent::AllToTorrentMessage>,
    /// send data to the remote peer
    writer: FramedWrite<OwnedWriteHalf, PeerProtocol>,
    /// the pieces the remote peer has
    peer_pieces: Pieces,
    /// the pieces we have
    my_pieces: Arc<RwLock<Pieces>>,
    /// the length of *blocks*, which are the subcomponent of pieces
    block_length: u32,
    /// the requests to the remote peer for data.
    requests_queue: AsyncQueue<Request>,
    cancellation_token: CancellationToken,
    i_am_choking_peer: bool,
    i_am_interested_in_peer: bool,
    peer_is_choking_me: bool,
    peer_is_interested_in_me: bool,
}

pub(crate) async fn from_socket(
    socket: tokio::net::TcpStream,
    remote_peer_id: PeerId,
    peer_args: PeerArgs,
) -> Result<PeerHandle, Error> {
    let global_permit = peer_args.global_max_connections.try_acquire_owned()?;
    debug!("got global permit");
    let torrent_permit = peer_args.max_torrent_connections.try_acquire_owned()?;
    debug!("got torrent permit");

    let ip = socket.peer_addr().unwrap().ip();
    let (socket_rx, mut socket_tx) = socket.into_split();
    let reader = FramedRead::new(socket_rx, protocol::PeerProtocol::new());
    let mut writer = FramedWrite::new(&mut socket_tx, protocol::HandshakeProtocol::new());

    timeout!(
        writer.send(HandshakeFrame::new(
            peer_args.info_hash,
            peer_args.my_id,
            [0; 8]
        )),
        15
    )
    .await??;
    debug!("sent handshake");

    let writer = FramedWrite::new(socket_tx, protocol::PeerProtocol::new());

    dbg!(&peer_args.pieces);

    let peer_handle = peer_loop(
        peer_args.pieces,
        peer_args.info_hash,
        peer_args.metainfo,
        peer_args.gossip_tx,
        peer_args.gossip_rx,
        &peer_args.data_path,
        peer_args.my_id,
        remote_peer_id,
        reader,
        writer,
        ip,
        peer_args.torrent_tx,
        peer_args.task_tracker,
        peer_args.cancellation_token,
        torrent_permit,
        global_permit,
    );

    Ok(peer_handle)
}

/// an outgoing connection: the way we connect to a remote peer
#[instrument(skip_all)]
pub(crate) async fn new(
    pieces: Arc<RwLock<Pieces>>,
    gossip_tx: broadcast::Sender<GossipMessage>,
    gossip_rx: broadcast::Receiver<GossipMessage>,
    data_path: &Path,
    ip: IpAddr,
    port: u16,
    info_hash: InfoHash,
    metainfo: Arc<MetaInfo>,
    my_id: PeerId,
    remote_peer_id: Option<PeerId>,
    torrent_tx: mpsc::Sender<torrent::AllToTorrentMessage>,
    task_tracker: TaskTracker,
    cancellation_token: CancellationToken,
    max_torrent_connections: Arc<Semaphore>,
    global_max_connections: Arc<Semaphore>,
) -> Result<PeerHandle, Error> {
    debug!("peer new");
    let global_permit = global_max_connections.try_acquire_owned()?;
    debug!("got global permit");
    let torrent_permit = max_torrent_connections.try_acquire_owned()?;
    debug!("got torrent permit");

    let mut socket = tokio::net::TcpStream::connect((ip, port)).await?;
    debug!("connected");

    let handshake_frame = handshake_peer(&mut socket, info_hash, my_id, remote_peer_id).await?;
    debug!("handshook peer");

    let (socket_rx, socket_tx) = socket.into_split();
    let reader = FramedRead::new(socket_rx, protocol::PeerProtocol::new());
    let writer = FramedWrite::new(socket_tx, PeerProtocol::new());
    debug!("reader and writer");

    let peer_handle = peer_loop(
        pieces,
        info_hash,
        metainfo,
        gossip_tx,
        gossip_rx,
        data_path,
        my_id,
        handshake_frame.peer_id,
        reader,
        writer,
        ip,
        torrent_tx,
        task_tracker,
        cancellation_token,
        torrent_permit,
        global_permit,
    );

    Ok(peer_handle)
}

/// the peer's main processing loop
#[instrument(skip_all)]
fn peer_loop(
    my_pieces: Arc<RwLock<Pieces>>,
    info_hash: InfoHash,
    metainfo: Arc<MetaInfo>,
    gossip_tx: broadcast::Sender<GossipMessage>,
    mut gossip_rx: broadcast::Receiver<GossipMessage>,
    data_path: &Path,
    my_id: PeerId,
    remote_peer_id: PeerId,
    mut reader: FramedRead<OwnedReadHalf, PeerProtocol>,
    mut writer: FramedWrite<OwnedWriteHalf, PeerProtocol>,
    ip: IpAddr,
    torrent_tx: mpsc::Sender<torrent::AllToTorrentMessage>,
    task_tracker: TaskTracker,
    cancellation_token: CancellationToken,
    torrent_permit: tokio::sync::OwnedSemaphorePermit,
    global_permit: tokio::sync::OwnedSemaphorePermit,
) -> PeerHandle {
    debug!("entered peer loop fn");
    let data_path = data_path.to_path_buf();

    let (peer_tx, mut peer_rx) = mpsc::channel(5);

    let task_handle = task_tracker.spawn(async move {
        debug!("entered peer loop task");
        let _global_permit = global_permit;
        let _torrent_permit = torrent_permit;

        {
            let my_pieces = my_pieces.read().await;
            timeout!(writer.send(Frame::Bitfield { pieces: my_pieces.to_owned() }), 5).await??;
            debug!("sent bitfield")
        }

        let mut keepalive_timer = interval(Duration::from_secs(30));
        keepalive_timer.reset();

        let mut requests_timer = interval(Duration::from_secs(1));
        requests_timer.reset();

        let mut inactivity_timer = interval(Duration::from_secs(120));
        inactivity_timer.reset();

        let pieces_length = {
            my_pieces.read().await.len()
        };

        let mut state = State {
            gossip_tx,
            metainfo,
            data_path,
            info_hash,
            my_id,
            torrent_tx,
            writer,
            peer_pieces: bitvec![u8, Msb0; 0; pieces_length],
            my_pieces,
            // TODO make this configurable
            block_length: 2u32.pow(14),
            requests_queue: AsyncQueue::new(10),
            cancellation_token,
            i_am_choking_peer: true,
            i_am_interested_in_peer: false,
            peer_is_choking_me: true,
            peer_is_interested_in_me: false,
        };


        loop {
            select! {
                _ = state.cancellation_token.cancelled() => {
                    debug!("{:?} shutting down peer connection task to {:?}", state.info_hash, remote_peer_id);
                    break;
                }
                Ok(m) = gossip_rx.recv() => {
                    match m {
                        GossipMessage::WeHave { index } => {
                            state.writer.send(Frame::Have { index }).await?;
                            debug!("told remote peer that I have {}", index);
                        },
                        GossipMessage::DownloadComplete => {
                            state.i_am_interested_in_peer = false;
                            state.writer.send(Frame::NotInterested).await?;
                        }
                        GossipMessage::Choke => {
                            if !state.i_am_choking_peer {
                                state.writer.send(Frame::Choke).await?;
                                state.i_am_choking_peer = true;
                            }
                        }
                    }
                }
                _ = requests_timer.tick() => {
                    maybe_enqueue_requests(&mut state).await?;
                }
                Some(request) = state.requests_queue.try_pop_front() => {
                    debug!("requesting piece {:?} from {:?}", request, remote_peer_id);
                    let _ = timeout!(state.writer.send(request.into()), 5).await?;
                }
                m = reader.next() => {
                    match m {
                        Some(Ok(frame)) => {
                            match frame {
                                protocol::Frame::Keepalive => (),
                                protocol::Frame::Choke => {
                                    debug!("received choke");
                                    handle_choke(&mut state).await?;
                                },
                                protocol::Frame::Unchoke => {
                                    debug!("received unchoke");
                                    handle_unchoke(&mut state).await?;
                                },
                                protocol::Frame::Interested => {
                                    debug!("received interested");
                                    handle_interested(&mut state).await?;
                                },
                                protocol::Frame::NotInterested => {
                                    debug!("received not interested");
                                    handle_not_interested(&mut state).await?;
                                },
                                protocol::Frame::Have { index } => {
                                    debug!("received have");
                                    handle_have(&mut state, index).await?;
                                },
                                protocol::Frame::Bitfield { pieces } => {
                                    debug!("received bitfield");
                                    handle_bitfield(&mut state, pieces).await?;
                                },
                                protocol::Frame::Request { index, begin, length } => {
                                    debug!("received request");
                                    handle_request(&mut state, index, begin, length).await?;
                                },
                                protocol::Frame::Piece { index, begin, block } => {
                                    debug!("received piece");
                                    handle_piece(&mut state, index, begin, &block).await?;
                                },
                                protocol::Frame::Cancel { index, begin, length } => {
                                    debug!("received cancel");
                                    handle_cancel(&mut state, index, begin, length).await?
                                },
                            }

                            inactivity_timer.reset();
                        }
                        Some(Err(e)) => {
                            debug!("got error reading from remote peer: {e}");
                            return Err(e.into())
                        },
                        None => {
                            debug!("got None reading from remote peer {:?}, disconnecting.", remote_peer_id);
                            let _ = state.torrent_tx.send(torrent::AllToTorrentMessage::PeerDisconnection { peer_id: remote_peer_id }).await;
                            break
                        }
                    }
                }
                m = peer_rx.recv() => {
                    match m {
                        None => break,
                        Some(m) => {
                            match m {
                                // TorrentToPeer::Choke => {
                                //     // if we ARE NOT choking peer, choke
                                //     if !state.i_am_choking_peer {
                                //         state.writer.send(Frame::Choke).await?;
                                //         state.i_am_choking_peer = true;
                                //     }
                                // }
                                TorrentToPeer::Unchoke => {
                                    // if we ARE choking peer, unchoke
                                    if state.i_am_choking_peer {
                                        state.writer.send(Frame::Unchoke).await?;
                                        state.i_am_choking_peer = false;
                                    }
                                }
                                TorrentToPeer::Shutdown => {
                                    debug!("peer received shutdown, shutting down");
                                    break
                                },
                            }
                        }
                    }
                }
                _ = keepalive_timer.tick() => {
                    state.writer.send(Frame::Keepalive).await?;
                }
                _ = inactivity_timer.tick() => {
                    debug!("did not receive any message from peer for 2 minutes, shutting down.");
                    break
                }
            }
        }

        Ok(())
    });

    PeerHandle {
        remote_peer_id,
        ip,
        peer_tx,
        task_handle,
    }
}
///  determine what pieces are in right that are not in left
fn right_but_not_left(left: &Pieces, right: &Pieces) -> Pieces {
    // NOTing left gives us the pieces left doesn't have.
    // ANDing those with right gives us the pieces that right has,
    // that left doesn't have.
    right.clone() & !left.clone()
}

#[instrument(skip_all)]
async fn handle_choke(state: &mut State) -> Result<(), Error> {
    state.peer_is_choking_me = true;
    state.requests_queue.clear();
    Ok(())
}

#[instrument(skip_all)]
async fn handle_unchoke(state: &mut State) -> Result<(), Error> {
    state.peer_is_choking_me = false;
    Ok(())
}

#[instrument(skip_all)]
async fn handle_interested(state: &mut State) -> Result<(), Error> {
    state.peer_is_interested_in_me = true;
    Ok(())
}

#[instrument(skip_all)]
async fn handle_not_interested(state: &mut State) -> Result<(), Error> {
    state.peer_is_interested_in_me = false;
    Ok(())
}

#[instrument(skip_all)]
async fn handle_have(state: &mut State, index: u32) -> Result<(), Error> {
    state.peer_pieces.set(index.try_into().unwrap(), true);

    let my_pieces = state.my_pieces.read().await;
    if peer_has_pieces_we_want(&my_pieces, &state.peer_pieces)? {
        state.i_am_interested_in_peer = true;
    }

    Ok(())
}

#[instrument(skip_all)]
async fn handle_bitfield(state: &mut State, peer_pieces: Pieces) -> Result<(), Error> {
    state.peer_pieces = peer_pieces;

    // let pieces = PIECES.read().await;
    // let my_pieces = pieces.get(&state.info_hash).ok_or(Error::NoPieces)?;
    let my_pieces = state.my_pieces.read().await;

    if right_but_not_left(&my_pieces, &state.peer_pieces).any() {
        debug!("i am interested because of bitfield");
        state.i_am_interested_in_peer = true;
        timeout!(state.writer.send(Frame::Interested), 5).await??;
    }

    Ok(())
}

#[instrument(skip(state))]
async fn handle_request(
    state: &mut State,
    index: u32,
    begin: u32,
    length: u32,
) -> Result<(), Error> {
    if state.i_am_choking_peer {
        return Ok(());
    }

    // let metainfos = METAINFOS.read().await;
    // let metainfo = metainfos.get(&state.info_hash).ok_or(Error::Metainfo)?;

    let mut write_location = state.data_path.clone();

    write_location.push(state.metainfo.name());

    let mut file = tokio::fs::File::options()
        .write(true)
        .read(true)
        .open(write_location)
        .await?;

    let block = download::read_block(&state.metainfo, &mut file, index, begin, length).await?;

    timeout!(
        state.writer.send(Frame::Piece {
            index,
            begin,
            block,
        }),
        3
    )
    .await??;

    Ok(())
}

#[instrument(skip(state, block))]
async fn handle_piece(
    state: &mut State,
    index: u32,
    begin: u32,
    block: &[u8],
) -> Result<(), Error> {
    debug!(
        index = ?index,
        begin = ?begin,
    );

    let mut write_location = state.data_path.clone();

    write_location.push(state.metainfo.name());

    let mut file = tokio::fs::File::options()
        .write(true)
        .read(true)
        .open(write_location)
        .await?;

    download::write_block(&state.metainfo, &mut file, index, begin, block).await?;

    if download::verify_piece(&state.metainfo, &mut file, index).await? {
        let mut my_pieces = timeout!(state.my_pieces.write(), 2).await?;
        my_pieces.set(index as usize, true);
        debug!("piece verified");
        state.gossip_tx.send(GossipMessage::WeHave { index })?;
    } else {
        debug!("piece did not verify");
    }

    Ok(())
}

#[instrument(skip(state))]
async fn handle_cancel(
    state: &mut State,
    index: u32,
    begin: u32,
    length: u32,
) -> Result<(), Error> {
    let to_remove = Request {
        index,
        begin,
        length,
    };

    state.requests_queue.filter(|request| request != &to_remove);

    Ok(())
}

pub(crate) async fn receive_handshake(
    socket: &mut tokio::net::TcpStream,
) -> Result<HandshakeFrame, Error> {
    let mut reader = FramedRead::new(socket, protocol::HandshakeProtocol::new());

    let handshake_frame = timeout!(reader.next(), 15)
        .await?
        .ok_or(Error::ConnectionClosed)??;
    debug!("received handshake, verifying");

    Ok(handshake_frame)
}

#[instrument(skip(socket))]
async fn handshake_peer(
    socket: &mut tokio::net::TcpStream,
    info_hash: InfoHash,
    my_id: PeerId,
    remote_peer_id: Option<PeerId>,
) -> Result<HandshakeFrame, Error> {
    let (rx, tx) = socket.split();

    let mut reader = FramedRead::new(rx, protocol::HandshakeProtocol::new());

    let mut writer = FramedWrite::new(tx, protocol::HandshakeProtocol::new());

    timeout!(
        writer.send(HandshakeFrame::new(info_hash, my_id, [0; 8])),
        15
    )
    .await??;
    debug!("sent handshake");

    let handshake_frame = timeout!(reader.next(), 15)
        .await?
        .ok_or(Error::ConnectionClosed)??;
    debug!("received handshake, verifying");

    if handshake_frame.info_hash != info_hash {
        return Err(Error::Handshake("info hash did not match"));
    }

    if let Some(remote_peer_id) = remote_peer_id {
        if handshake_frame.peer_id != remote_peer_id {
            return Err(Error::Handshake("remote peer id did not match"));
        }
    }

    debug!("handshake is good");

    Ok(handshake_frame)
}

async fn maybe_enqueue_requests(state: &mut State) -> Result<(), Error> {
    if should_download(state) {
        let unhad_pieces = {
            let my_pieces = state.my_pieces.read().await;
            unhad_pieces(&my_pieces, &state.peer_pieces)?
        };

        // let metainfos = METAINFOS.read().await;
        // let metainfo = metainfos.get(&state.info_hash).ok_or(Error::Metainfo)?;

        let pieces_we_dont_have = unhad_pieces.iter_ones().take(state.requests_queue.capacity);

        let pieces_and_blocks_we_dont_have = pieces_we_dont_have.map(|index| {
            (
                index,
                state
                    .metainfo
                    .blocks_for_piece(u32::try_from(index).unwrap(), state.block_length),
            )
        });

        'outer: for (index, blocks) in pieces_and_blocks_we_dont_have {
            for block in blocks {
                let request = Request {
                    index: index.try_into().unwrap(),
                    begin: block.begin,
                    length: block.length,
                };

                debug!("enqueing request {:?} to download", request);

                // once we are out of capacity, break out of the loop
                if !state.requests_queue.try_push_back(request) {
                    break 'outer;
                };
            }
        }
    }

    Ok(())
}

fn should_download(state: &mut State) -> bool {
    state.i_am_interested_in_peer && !state.peer_is_choking_me
}

fn unhad_pieces(my_pieces: &Pieces, peer_pieces: &Pieces) -> Result<Pieces, Error> {
    Ok(right_but_not_left(my_pieces, peer_pieces))
}

fn peer_has_pieces_we_want(my_pieces: &Pieces, peer_pieces: &Pieces) -> Result<bool, Error> {
    Ok(unhad_pieces(my_pieces, peer_pieces)?.any())
}

#[derive(Debug)]
pub(crate) struct PeerHandle {
    /// the id of the remote peer
    pub remote_peer_id: PeerId,
    pub ip: IpAddr,
    /// this is the channel by which we get messages from the torrent task to the peer task
    peer_tx: mpsc::Sender<TorrentToPeer>,
    /// in case we need to kill the task or figure out why it panicked
    task_handle: JoinHandle<Result<(), Error>>,
}

impl PeerHandle {
    #[instrument]
    pub(crate) async fn shutdown(&self) {
        if let Err(_elapsed) = timeout!(self.peer_tx.send(TorrentToPeer::Shutdown), 5).await {
            self.task_handle.abort();
        }
    }

    pub(crate) async fn unchoke(&self) {
        if let Err(_elapsed) = timeout!(self.peer_tx.send(TorrentToPeer::Unchoke), 5).await {
            self.task_handle.abort();
        }
    }

    // pub(crate) async fn choke(&self) {
    //     if let Err(_elapsed) = timeout!(self.peer_tx.send(TorrentToPeer::Choke), 5).await {
    //         self.task_handle.abort();
    //     }
    // }
}

#[cfg(test)]
mod tests {

    use crate::torrent::Pieces;

    use super::right_but_not_left;

    #[test]
    fn diffs_bitfields() {
        let mut peer_pieces = Pieces::new();
        peer_pieces.resize(5, false);

        let mut my_pieces = Pieces::new();
        my_pieces.resize(5, false);

        // set the middle bit in peer_pieces
        peer_pieces.set(2, true);
        // and set the first bit in my_pieces
        my_pieces.set(0, true);

        let diff = right_but_not_left(&my_pieces, &peer_pieces);
        assert_eq!(diff.get(0).unwrap(), false);
        assert_eq!(diff.get(1).unwrap(), false);
        assert_eq!(diff.get(2).unwrap(), true);
        assert_eq!(diff.get(3).unwrap(), false);
        assert_eq!(diff.get(4).unwrap(), false);
    }
}
