use crate::torrent::{self, PeerId, Pieces};
use crate::{timeout, InfoHash};
use bitvec::{bitvec, order::Msb0};
use futures::sink::SinkExt;
use protocol::{Frame, HandshakeFrame, PeerProtocol};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, TryAcquireError};
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
    #[error("connection reset")]
    ConnectionReset,
    #[error("handshake")]
    Handshake(&'static str),
    #[error("protocol error")]
    Protocol(#[from] protocol::Error),
    #[error("no more available connections")]
    NoAvailableConnections(#[from] TryAcquireError),
}

pub enum TorrentToPeer {
    Shutdown,
}

// TODO implement Debug manually for this, since it contains undebuggable fields
struct State {
    /// me
    my_id: PeerId,
    /// send messages to the owning torrent
    torrent_tx: mpsc::Sender<torrent::Message>,
    /// send data to the remote peer
    writer: FramedWrite<OwnedWriteHalf, PeerProtocol>,
    /// the pieces the remote peer has
    peer_pieces: Pieces,
    /// the pieces we have
    my_pieces: Pieces,
    i_am_choking_peer: bool,
    i_am_interested_in_peer: bool,
    peer_is_choking_me: bool,
    peer_is_interested_in_me: bool,
}

#[instrument(skip_all)]
pub(crate) async fn new(
    ip: IpAddr,
    port: u16,
    info_hash: InfoHash,
    my_id: PeerId,
    pieces: Pieces,
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
        my_id,
        handshake_frame.peer_id,
        reader,
        writer,
        ip,
        torrent_tx,
        pieces,
        permit,
        torrent_permit,
    );

    Ok(peer_handle)
}

pub(crate) async fn accept_peer_connection(socket: TcpStream) -> Result<PeerHandle, Error> {
    todo!()
}

/// the peer's main processing loop
#[instrument(skip_all)]
fn peer_loop(
    info_hash: InfoHash,
    my_id: PeerId,
    remote_peer_id: PeerId,
    mut reader: FramedRead<OwnedReadHalf, PeerProtocol>,
    mut writer: FramedWrite<OwnedWriteHalf, PeerProtocol>,
    ip: IpAddr,
    torrent_tx: mpsc::Sender<torrent::Message>,
    my_pieces: Pieces,
    permit: tokio::sync::SemaphorePermit<'static>,
    torrent_permit: tokio::sync::OwnedSemaphorePermit,
) -> PeerHandle {
    debug!("entered peer loop fn");

    let (peer_tx, mut peer_rx) = mpsc::channel(10);

    let rt = tokio::runtime::Handle::current();

    let task_handle = rt.spawn(async move {
        debug!("entered peer loop task");
        let _permit = permit;
        let _torrent_permit = torrent_permit;

        // send the initial bitfield, we do not treat this as part of the handshake
        {
            // get a read lock on the torrents that only lives for this scope,
            // so we don't hold it for the life of the whole task (which is indefinite)
            let torrents = timeout!(crate::TORRENTS.read(), 5).await?;

            if let Some(torrent) = torrents.get(&info_hash) {
                if let Ok(my_pieces) = torrent.get_pieces().await {
                    timeout!(writer.send(Frame::Bitfield { pieces: my_pieces }), 5).await??;
                    debug!("sent bitfield")
                } else {
                    return Ok(());
                };
            } else {
                return Ok(());
            }
        };

        let mut timeout_timer = interval(Duration::from_secs(30));
        timeout_timer.reset();

        let mut state = State {
            my_id,
            torrent_tx,
            writer,
            peer_pieces: bitvec![u8, Msb0; 0; my_pieces.len()],
            my_pieces,
            i_am_choking_peer: true,
            i_am_interested_in_peer: false,
            peer_is_choking_me: true,
            peer_is_interested_in_me: false,
        };

        loop {
            select! {
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
                                    handle_piece(&state, index, begin, &block).await?;
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
    // TODO
    // figure out what pieces to download
    // https://docs.rs/bitvec/1.0.1/bitvec/vec/struct.BitVec.html#method.iter_ones
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
    Ok(())
}

#[instrument(skip_all)]
async fn handle_bitfield(state: &mut State, peer_pieces: Pieces) -> Result<(), Error> {
    state.peer_pieces = peer_pieces;

    if right_but_not_left(&state.my_pieces, &state.peer_pieces).any() {
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

#[instrument(skip(state))]
async fn handle_piece(state: &State, index: u32, begin: u32, block: &[u8]) -> Result<(), Error> {
    todo!()
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

    // TODO do not unwrap here
    let handshake_frame = timeout!(reader.next(), 15).await?.unwrap()?;

    if handshake_frame.info_hash != info_hash {
        return Err(Error::Handshake("info hash did not match"));
    }

    if let Some(remote_peer_id) = remote_peer_id {
        if handshake_frame.peer_id != remote_peer_id {
            return Err(Error::Handshake("remote peer id did not match"));
        }
    }

    debug!("received handshake");

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
