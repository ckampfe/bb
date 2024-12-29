use crate::torrent::{self, P2PMessage, PeerId, Pieces};
use crate::{download, timeout, InfoHash, METAINFOS, PIECES};
use bitvec::{bitvec, order::Msb0};
use futures::sink::SinkExt;
use protocol::{Frame, HandshakeFrame, PeerProtocol};
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc, TryAcquireError};
use tokio::time::interval;
use tokio::{select, task::JoinHandle, time::error::Elapsed};
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, FramedWrite};
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
    P2PBroadcast(#[from] tokio::sync::broadcast::error::SendError<P2PMessage>),
    #[error("unable to get pieces for this torrent")]
    NoPieces,
}

pub enum TorrentToPeer {
    Shutdown,
}

#[derive(Debug)]
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
/// 2. we can eventually implement cancel, as the requests are reified objects
#[derive(Debug)]
struct AsyncQueue<T> {
    capacity: usize,
    tx: mpsc::Sender<T>,
    rx: mpsc::Receiver<T>,
}

impl<T> AsyncQueue<T> {
    fn new(capacity: usize) -> Self {
        let (tx, rx) = mpsc::channel(capacity);
        Self { capacity, tx, rx }
    }

    /// either enqueues the item or returns immediately
    /// if there is no remaining capacity
    async fn try_push_back(
        &mut self,
        item: T,
    ) -> Result<(), tokio::sync::mpsc::error::TrySendError<T>> {
        self.tx.try_send(item)
    }

    /// either dequeues an item or returns immediately
    /// if there is no item
    async fn try_pop_front(&mut self) -> Result<T, tokio::sync::mpsc::error::TryRecvError> {
        self.rx.try_recv()
    }
}

// TODO implement Debug manually for this, since it contains undebuggable fields
struct State {
    info_hash: InfoHash,
    /// used to communicate status information to other peer tasks
    p2p_tx: broadcast::Sender<P2PMessage>,
    /// where the downloaded data lives
    // TODO make this updatable by the torrent task
    data_path: PathBuf,
    /// me
    my_id: PeerId,
    /// send messages to the owning torrent
    torrent_tx: mpsc::Sender<torrent::Message>,
    /// send data to the remote peer
    writer: FramedWrite<OwnedWriteHalf, PeerProtocol>,
    /// the pieces the remote peer has
    peer_pieces: Pieces,
    /// the length of *blocks*, which are the subcomponent of pieces
    block_length: u32,
    /// the requests to the remote peer for data.
    requests_queue: AsyncQueue<Request>,
    i_am_choking_peer: bool,
    i_am_interested_in_peer: bool,
    peer_is_choking_me: bool,
    peer_is_interested_in_me: bool,
}

/// an outgoing connection: the way we connect to a remote peer
#[instrument(skip_all)]
pub(crate) async fn new(
    p2p_tx: broadcast::Sender<P2PMessage>,
    p2p_rx: broadcast::Receiver<P2PMessage>,
    data_path: &Path,
    ip: IpAddr,
    port: u16,
    info_hash: InfoHash,
    my_id: PeerId,
    remote_peer_id: Option<PeerId>,
    torrent_tx: mpsc::Sender<torrent::Message>,
    max_peer_connections: Arc<tokio::sync::Semaphore>,
) -> Result<PeerHandle, Error> {
    debug!("peer new");
    let permit = crate::GLOBAL_MAX_CONNECTIONS.try_acquire()?;
    debug!("got global permit");
    let torrent_permit = max_peer_connections.try_acquire_owned()?;
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
        info_hash,
        p2p_tx,
        p2p_rx,
        data_path,
        my_id,
        handshake_frame.peer_id,
        reader,
        writer,
        ip,
        torrent_tx,
        permit,
        torrent_permit,
    );

    Ok(peer_handle)
}

/// an incoming connection: the way a remote peer connects to us
pub(crate) async fn accept_peer_connection(socket: TcpStream) -> Result<PeerHandle, Error> {
    todo!()
}

/// the peer's main processing loop
#[instrument(skip_all)]
fn peer_loop(
    info_hash: InfoHash,
    p2p_tx: broadcast::Sender<P2PMessage>,
    mut p2p_rx: broadcast::Receiver<P2PMessage>,
    data_path: &Path,
    my_id: PeerId,
    remote_peer_id: PeerId,
    mut reader: FramedRead<OwnedReadHalf, PeerProtocol>,
    mut writer: FramedWrite<OwnedWriteHalf, PeerProtocol>,
    ip: IpAddr,
    torrent_tx: mpsc::Sender<torrent::Message>,
    permit: tokio::sync::SemaphorePermit<'static>,
    torrent_permit: tokio::sync::OwnedSemaphorePermit,
) -> PeerHandle {
    debug!("entered peer loop fn");
    let data_path = data_path.to_path_buf();

    let (peer_tx, mut peer_rx) = mpsc::channel(10);

    let rt = tokio::runtime::Handle::current();

    let task_handle = rt.spawn(async move {
        debug!("entered peer loop task");
        let _permit = permit;
        let _torrent_permit = torrent_permit;

        // send the initial bitfield, we do not treat this as part of the handshake
        {
            let pieces = PIECES.read().await;
            let my_pieces = pieces.get(&info_hash).ok_or(Error::NoPieces)?;
            timeout!(writer.send(Frame::Bitfield { pieces: my_pieces.to_owned() }), 5).await??;
            debug!("sent bitfield")
        }

        let mut timeout_timer = interval(Duration::from_secs(30));
        timeout_timer.reset();

        let mut requests_timer = interval(Duration::from_secs(3));
        requests_timer.reset();

        let pieces_length =  {
            let pieces = PIECES.read().await;
            pieces.get(&info_hash).map(|pieces| pieces.len()).ok_or(Error::NoPieces)?
        };

        let mut state = State {
            p2p_tx,
            data_path,
            info_hash,
            my_id,
            torrent_tx,
            writer,
            peer_pieces: bitvec![u8, Msb0; 0; pieces_length],
            // TODO make this configurable
            block_length: 2u32.pow(14),
            requests_queue: AsyncQueue::new(10),
            i_am_choking_peer: true,
            i_am_interested_in_peer: false,
            peer_is_choking_me: true,
            peer_is_interested_in_me: false,
        };

        loop {
            select! {
                Ok(m) = p2p_rx.recv() => {
                    match m {
                        P2PMessage::Have { index } => {
                            state.writer.send(Frame::Have { index }).await?;
                            debug!("told remote peer that I have {}", index);
                        },
                    }
                }
                _ = requests_timer.tick() => {
                    maybe_enqueue_requests(&mut state).await?;
                }
                Ok(request) = state.requests_queue.try_pop_front() => {
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
                                    handle_request(&state, index, begin, length).await?;
                                },
                                protocol::Frame::Piece { index, begin, block } => {
                                    debug!("received piece");
                                    handle_piece(&mut state, index, begin, &block).await?;
                                },
                                protocol::Frame::Cancel { index, begin, length } => {
                                    debug!("received cancel");
                                    handle_cancel(&state, index, begin, length).await?
                                },
                            }
                        }
                        Some(Err(e)) => {
                            debug!("got error reading from remote peer: {e}");
                            return Err(e.into())
                        },
                        None => {
                            debug!("got None reading from remote peer {:?}, disconnecting.", remote_peer_id);
                            let _ = state.torrent_tx.send(torrent::Message::PeerDisconnection { peer_id: remote_peer_id }).await;
                            break
                        }
                    }
                }
                m = peer_rx.recv() => {
                    match m {
                        None => break,
                        Some(m) => {
                            match m {
                                TorrentToPeer::Shutdown => {
                                    debug!("peer received shutdown, shutting down");
                                    break
                                },
                            }
                        }
                    }
                }
                _ = timeout_timer.tick() => {
                    state.writer.send(Frame::Keepalive).await?;
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
    // TODO cancel any queued requests
    Ok(())
}

#[instrument(skip_all)]
async fn handle_unchoke(state: &mut State) -> Result<(), Error> {
    state.peer_is_choking_me = false;
    Ok(())
}

async fn maybe_enqueue_requests(state: &mut State) -> Result<(), Error> {
    if should_download(state) {
        let unhad_pieces = {
            let pieces = PIECES.read().await;
            let my_pieces = pieces.get(&state.info_hash).ok_or(Error::NoPieces)?;
            right_but_not_left(my_pieces, &state.peer_pieces)
        };

        if let Some(metainfo) = METAINFOS.read().await.get(&state.info_hash) {
            let pieces_we_dont_have = unhad_pieces.iter_ones().take(state.requests_queue.capacity);

            let pieces_and_blocks_we_dont_have = pieces_we_dont_have.map(|index| {
                (
                    index,
                    metainfo.blocks_for_piece(u32::try_from(index).unwrap(), state.block_length),
                )
            });

            for (index, blocks) in pieces_and_blocks_we_dont_have {
                for block in blocks {
                    let request = Request {
                        index: index.try_into().unwrap(),
                        begin: block.begin,
                        length: block.length,
                    };

                    debug!("enqueing request {:?} to download", request);

                    // if we can't, ignore.
                    let _ = state.requests_queue.try_push_back(request).await;
                }
            }
        }
    }

    Ok(())
}

fn should_download(state: &mut State) -> bool {
    state.i_am_interested_in_peer && !state.peer_is_choking_me
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
    Ok(())
}

#[instrument(skip_all)]
async fn handle_bitfield(state: &mut State, peer_pieces: Pieces) -> Result<(), Error> {
    state.peer_pieces = peer_pieces;

    let pieces = PIECES.read().await;
    let my_pieces = pieces.get(&state.info_hash).ok_or(Error::NoPieces)?;

    if right_but_not_left(my_pieces, &state.peer_pieces).any() {
        debug!("i am interested because of bitfield");
        state.i_am_interested_in_peer = true;
        timeout!(state.writer.send(Frame::Interested), 5).await??;
    }

    Ok(())
}

#[instrument(skip(state))]
async fn handle_request(state: &State, index: u32, begin: u32, length: u32) -> Result<(), Error> {
    todo!()
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

    if let Some(metainfo) = METAINFOS.read().await.get(&state.info_hash) {
        debug!("here1");
        let mut write_location = state.data_path.clone();

        write_location.push(metainfo.name());

        let mut file = tokio::fs::File::options()
            .write(true)
            .read(true)
            .open(write_location)
            .await?;

        download::write_block(metainfo, &mut file, index, begin, block).await?;

        if download::verify_piece(metainfo, &mut file, index).await? {
            let mut pieces = timeout!(PIECES.write(), 2).await?;
            let my_pieces = pieces.get_mut(&state.info_hash).ok_or(Error::NoPieces)?;
            my_pieces.set(index as usize, true);
            debug!("piece verified");
            state.p2p_tx.send(P2PMessage::Have { index })?;
        } else {
            debug!("piece did not verify");
        }
    } else {
        return Err(Error::Metainfo);
    }

    Ok(())
}

#[instrument(skip(state))]
async fn handle_cancel(state: &State, index: u32, begin: u32, length: u32) -> Result<(), Error> {
    todo!()
}

// TODO have this use its own codec
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

#[derive(Debug)]
pub(crate) struct PeerHandle {
    /// the id of the remote peer
    pub remote_peer_id: PeerId,
    pub ip: IpAddr,
    /// this is the channel by which we get messages to the peer
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
