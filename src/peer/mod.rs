use crate::torrent::{PeerId, Pieces};
use crate::{timeout, InfoHash};
use bitvec::{bitvec, order::Msb0};
use bytes::{Buf, BufMut};
use futures::sink::SinkExt;
use protocol::{Frame, PeerProtocol};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::sync::{mpsc, TryAcquireError};
use tokio::{select, task::JoinHandle, time::error::Elapsed};
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, FramedWrite};

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

pub enum PeerToTorrent {}

pub enum TorrentToPeer {
    Shutdown,
}

struct State {
    /// me
    my_id: PeerId,
    /// send messages to the owning torrent
    torrent_tx: mpsc::Sender<PeerToTorrent>,
    /// the pieces the remote peer has
    peer_pieces: Pieces,
    i_am_choking_peer: bool,
    i_am_interested_in_peer: bool,
    peer_is_choking_me: bool,
    peer_is_interested_in_me: bool,
}

pub(crate) async fn new<A: ToSocketAddrs>(
    addr: A,
    info_hash: InfoHash,
    my_id: PeerId,
    pieces: Pieces,
    remote_peer_id: PeerId,
    torrent_tx: mpsc::Sender<PeerToTorrent>,
) -> Result<PeerHandle, Error> {
    let permit = crate::GLOBAL_MAX_CONNECTIONS.try_acquire()?;

    let mut socket = tokio::net::TcpStream::connect(addr).await?;

    handshake_peer(&mut socket, info_hash, my_id, remote_peer_id).await?;

    let (socket_rx, socket_tx) = socket.into_split();

    let reader = FramedRead::new(socket_rx, protocol::PeerProtocol::new());

    let writer = FramedWrite::new(socket_tx, PeerProtocol::new());

    let peer_handle = peer_loop(info_hash, my_id, reader, writer, torrent_tx, pieces, permit);

    Ok(peer_handle)
}

pub(crate) async fn accept_peer_connection(socket: TcpStream) -> Result<PeerHandle, Error> {
    todo!()
}

/// the peer's main processing loop
fn peer_loop(
    info_hash: InfoHash,
    my_id: PeerId,
    mut reader: FramedRead<OwnedReadHalf, PeerProtocol>,
    mut writer: FramedWrite<OwnedWriteHalf, PeerProtocol>,
    torrent_tx: mpsc::Sender<PeerToTorrent>,
    pieces: Pieces,
    permit: tokio::sync::SemaphorePermit<'static>,
) -> PeerHandle {
    let (peer_tx, mut peer_rx) = mpsc::channel(10);

    let rt = tokio::runtime::Handle::current();

    let task_handle = rt.spawn(async move {
        let _permit = permit;

        // send the initial bitfield, we do not treat this as part of the handshake
        {
            // get a read lock on the torrents that only lives for this scope,
            // so we don't hold it for the life of the whole task (which is indefinite)
            let torrents = timeout!(crate::TORRENTS.read(), 5).await?;

            if let Some(torrent) = torrents.get(&info_hash) {
                if let Ok(my_pieces) = torrent.get_pieces().await {
                    timeout!(writer.send(Frame::Bitfield { pieces: my_pieces }), 5).await??;
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
            peer_pieces: bitvec![u8, Msb0; 0; pieces.len()],
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
                                    state.peer_pieces = pieces
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
                                TorrentToPeer::Shutdown => break,
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    });

    PeerHandle {
        peer_tx,
        task_handle,
    }
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
async fn handshake_peer(
    socket: &mut tokio::net::TcpStream,
    info_hash: InfoHash,
    my_id: PeerId,
    remote_peer_id: PeerId,
) -> Result<(), Error> {
    send_handshake(socket, info_hash, my_id).await?;
    receive_handshake(socket, info_hash, remote_peer_id).await?;
    Ok(())
}

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
    Ok(())
}

async fn receive_handshake(
    socket: &mut tokio::net::TcpStream,
    info_hash: InfoHash,
    remote_peer_id: PeerId,
) -> Result<(), Error> {
    let mut buf = [0; 49 + 19];

    socket.read_exact(&mut buf).await?;

    let mut buf = &buf[..];

    if 19 != buf.get_u8() {
        return Err(Error::Handshake("did not get 19 byte for handshake"));
    }
    let mut take = Buf::take(buf, 19);
    let mut proto = vec![0; 19];
    proto.put(&mut take);

    if b"BitTorrent protocol" != &proto[..] {
        return Err(Error::Handshake("did not get 'BitTorrent protocol'"));
    }

    let mut take = Buf::take(buf, 8);
    let mut reserved = vec![0; 8];
    reserved.put(&mut take);

    let mut take = Buf::take(buf, 20);
    let mut challenge_info_hash = vec![0; 20];
    challenge_info_hash.put(&mut take);

    if info_hash.0 != challenge_info_hash[..] {
        return Err(Error::Handshake("info hash did not match"));
    }

    // TODO move this out so we can accept connections from peers
    // without knowing their peer ids already
    let mut take = Buf::take(buf, 20);
    let mut challenge_peer_id = vec![0; 20];
    challenge_peer_id.put(&mut take);

    if remote_peer_id.0 != challenge_peer_id[..] {
        return Err(Error::Handshake("peer id did not match"));
    }

    Ok(())
}

#[derive(Debug)]
pub(crate) struct PeerHandle {
    /// this is the channel by which we get messages to the peer
    peer_tx: mpsc::Sender<TorrentToPeer>,
    /// in case we need to kill the task or figure out why it panicked
    task_handle: JoinHandle<Result<(), Error>>,
}

impl PeerHandle {
    async fn shutdown(&self) -> Result<(), Error> {
        Ok(timeout!(self.peer_tx.send(TorrentToPeer::Shutdown), 5).await??)
    }
}
