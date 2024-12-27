use crate::torrent::{self, PeerId, Pieces};
use crate::{timeout, InfoHash};
use bitvec::{bitvec, order::Msb0};
use futures::sink::SinkExt;
use protocol::{Frame, PeerProtocol};
use std::net::IpAddr;
use std::sync::Arc;
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, TryAcquireError};
use tokio::{select, task::JoinHandle, time::error::Elapsed};
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{debug, instrument};

mod protocol;

const TWO_MINUTES: u64 = 2 * 60;

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

struct State {
    /// me
    my_id: PeerId,
    /// send messages to the owning torrent
    torrent_tx: mpsc::Sender<torrent::Message>,
    /// the pieces the remote peer has
    peer_pieces: Pieces,
    my_pieces: Pieces,
    i_am_choking_peer: bool,
    i_am_interested_in_peer: bool,
    peer_is_choking_me: bool,
    peer_is_interested_in_me: bool,
}

#[instrument]
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

    let actual_remote_peer_id =
        handshake_peer(&mut socket, info_hash, my_id, remote_peer_id).await?;
    debug!("handshook peer");

    let (socket_rx, socket_tx) = socket.into_split();
    let reader = FramedRead::new(socket_rx, protocol::PeerProtocol::new());
    let writer = FramedWrite::new(socket_tx, PeerProtocol::new());
    debug!("reader and writer");

    let peer_handle = peer_loop(
        info_hash,
        my_id,
        actual_remote_peer_id,
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
                    return Ok(())
                };
            } else {
                return Ok(());
            }
        };

        let mut state = State {
            my_id,
            torrent_tx,
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
                                protocol::Frame::Choke => state.peer_is_choking_me = true,
                                protocol::Frame::Unchoke => state.peer_is_choking_me = false,
                                protocol::Frame::Interested => state.peer_is_interested_in_me = true,
                                protocol::Frame::NotInterested => state.peer_is_interested_in_me = false,
                                protocol::Frame::Have { index } => {
                                    state.peer_pieces.set(index.try_into().unwrap(), true)
                                },
                                protocol::Frame::Bitfield { pieces } => {
                                    debug!("received bitfield");

                                    state.peer_pieces = pieces;

                                    if right_but_not_left(&state.my_pieces, &state.peer_pieces).any() {
                                        debug!("i am interested because of bitfield");
                                        state.i_am_interested_in_peer = true;
                                    }

                                    if state.i_am_interested_in_peer {
                                        // TODO
                                        // figure out what pieces to download
                                        // https://docs.rs/bitvec/1.0.1/bitvec/vec/struct.BitVec.html#method.iter_ones
                                    }
                                },
                                protocol::Frame::Request { index, begin, length } => {
                                    handle_request(&state, index, begin, length).await?;
                                },
                                protocol::Frame::Piece { index, begin, block } => {
                                    handle_piece(&state, index, begin, &block).await?;
                                },
                                protocol::Frame::Cancel { index, begin, length } => handle_cancel(&state, index, begin, length).await?,
                            }
                        }
                        Some(Err(e)) => return Err(e.into()),
                        None => break
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
    right.clone() & !left.clone()
}

async fn handle_request(state: &State, index: u32, begin: u32, length: u32) -> Result<(), Error> {
    todo!()
}

async fn handle_piece(state: &State, index: u32, begin: u32, block: &[u8]) -> Result<(), Error> {
    todo!()
}

async fn handle_cancel(state: &State, index: u32, begin: u32, length: u32) -> Result<(), Error> {
    todo!()
}

// TODO have this use its own codec
#[instrument]
async fn handshake_peer(
    socket: &mut tokio::net::TcpStream,
    info_hash: InfoHash,
    my_id: PeerId,
    remote_peer_id: Option<PeerId>,
) -> Result<PeerId, Error> {
    timeout!(send_handshake(socket, info_hash, my_id), 5).await??;

    let mut reader = FramedRead::new(
        socket,
        protocol::HandshakeProtocol::new(info_hash, remote_peer_id),
    );

    // TODO do not unwrap here
    let peer_id = timeout!(reader.next(), 15).await?.unwrap()?;
    debug!("handshake good");

    Ok(peer_id)
}

// TODO do this as a codec
async fn send_handshake(
    socket: &mut tokio::net::TcpStream,
    info_hash: InfoHash,
    my_id: PeerId,
) -> Result<(), Error> {
    socket.write_u8(19).await?;
    socket.write_all(b"BitTorrent protocol").await?;
    socket.write_all(&[0, 0, 0, 0, 0, 0, 0, 0]).await?;
    socket.write_all(&info_hash.0).await?;
    socket.write_all(&my_id.0).await?;
    socket.flush().await?;
    debug!("sent handshake");
    Ok(())
}

#[derive(Debug)]
pub(crate) struct PeerHandle {
    /// the id of the remote peer
    remote_peer_id: PeerId,
    pub ip: IpAddr,
    /// this is the channel by which we get messages to the peer
    peer_tx: mpsc::Sender<TorrentToPeer>,
    /// in case we need to kill the task or figure out why it panicked
    task_handle: JoinHandle<Result<(), Error>>,
}

impl PeerHandle {
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
