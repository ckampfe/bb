use crate::bencode::{self, Bencode};
use crate::metainfo::MetaInfo;
use crate::peer::{self, PeerHandle};
use crate::{timeout, InfoHash, METAINFOS};
use base64::Engine;
use bitvec::prelude::*;
use bitvec::vec::BitVec;
use rand::Rng;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashSet};
use std::fmt::{Debug, Display};
use std::net::IpAddr;
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::select;
use tokio::sync::{mpsc, Semaphore};
use tokio::time::Interval;
use tracing::debug;

pub type Pieces = BitVec<u8, bitvec::order::Msb0>;

/// uniquely identifies a peer
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct PeerId(pub [u8; 20]);

impl Debug for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let base64 = base64::prelude::BASE64_STANDARD.encode(self.0);
        write!(f, "{}", base64)
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("could not read .torrent file")]
    DotTorrentRead(String),
    #[error("bencode error")]
    Bencode(#[from] bencode::Error),
    #[error("oneshot channel error")]
    OneshotChannelRecv(#[from] tokio::sync::oneshot::error::RecvError),
    #[error("coudl not send on channel")]
    MpscSend(#[from] tokio::sync::mpsc::error::SendError<Message>),
    #[error("timeout")]
    Timeout(#[from] tokio::time::error::Elapsed),
    #[error("announce")]
    Announce(#[from] reqwest::Error),
    #[error("io")]
    Io(#[from] std::io::Error),
    #[error("the metainfo no longer exists for some reasons")]
    NoMetainfo,
}

pub(crate) enum Message {
    GetState {
        reply_tx: tokio::sync::oneshot::Sender<Result<String, Error>>,
    },
    Announce {
        reply_tx: tokio::sync::oneshot::Sender<Result<Bencode, Error>>,
    },
    GetPieces {
        reply_tx: tokio::sync::oneshot::Sender<Result<Pieces, Error>>,
    },
    PeerDisconnection {
        peer_id: PeerId,
    },
    VerifyLocalData,
}

#[derive(Debug)]
struct State {
    /// the channel to communicate to the runtime itself, if need be
    // rt_tx: tokio::sync::mpsc::Sender<runtime::Message>,
    info_hash: InfoHash,
    /// location of the .torrent
    dot_torrent_path: PathBuf,
    /// download location
    data_path: PathBuf,
    /// for tracker announce/scrape/etc
    http_client: reqwest::Client,
    /// the pieces of the torrent that we have
    pieces: Pieces,
    /// peer's we're connected to
    connected_peers: Vec<PeerHandle>,
    /// peers, as we get them from the tracker
    /// a hashet ensures that we only ever have one entry per unique peer
    available_peers: HashSet<AvailablePeer>,
    peer_stats: BTreeMap<PeerId, PeerStats>,
    /// the last announce we've received from the tracker
    last_announce: Option<Bencode>,
    // ("peer_id", self.peer_id),
    my_id: PeerId,
    // ("port", self.port),
    port: u16,
    // ("left", self.left),
    left_bytes: u64,
    // ("uploaded", self.uploaded),
    uploaded_bytes: u64,
    // ("downloaded", self.downloaded),
    downloaded_bytes: u64,
    // ("event", self.state),
    state: TrackerState,
    max_peer_connections: Arc<Semaphore>,
    torrent_tx: mpsc::Sender<Message>,
}

/// stats for a single peer
#[derive(Debug)]
struct PeerStats {
    downloaded: usize,
    uploaded: usize,
}

#[derive(Debug)]
enum TrackerState {
    Started,
    Stopped,
    Completed,
}

impl Display for TrackerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let out = match self {
            TrackerState::Started => "started",
            TrackerState::Stopped => "stopped",
            TrackerState::Completed => "completed",
        };

        write!(f, "{}", out)
    }
}

pub struct Options {
    port: Port,
    max_peer_connections: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            port: Port::Port(6881),
            max_peer_connections: 40,
        }
    }
}

enum Port {
    Port(u16),
    Random,
}

pub(crate) async fn new<P: AsRef<Path>>(
    dot_torrent_path: P,
    data_path: P,
    options: Options,
) -> Result<(crate::InfoHash, TorrentHandle), Error> {
    let dot_torrent_data = tokio::fs::read(&dot_torrent_path)
        .await
        .map_err(|e| Error::DotTorrentRead(e.to_string()))?;
    let dot_torrent_decoded = bencode::decode(&dot_torrent_data).unwrap();
    let metainfo = MetaInfo::new(dot_torrent_decoded);
    let info_hash = metainfo.info_hash()?;
    let number_of_pieces = usize::try_from(metainfo.number_of_pieces()).unwrap();
    // TODO actually calculate the number of bytes left
    let left_bytes = metainfo.length();

    // only take the write lock for this block
    {
        let mut metainfos = crate::METAINFOS.write().await;
        metainfos.insert(info_hash, metainfo);
    }

    let dot_torrent_path = dot_torrent_path.as_ref().to_path_buf();
    let data_path = data_path.as_ref().to_path_buf();

    // TODO create file if it doesn't exist

    // TODO randomize this in release
    let mut peer_id_bytes = [0u8; 20];
    // rand::thread_rng().fill_bytes(&mut peer_id_bytes);
    let peer_id = PeerId(peer_id_bytes);

    let port = match options.port {
        Port::Port(port) => port,
        Port::Random => rand::thread_rng().gen::<u16>(),
    };

    let max_peer_connections = Arc::new(Semaphore::new(options.max_peer_connections));

    let torrent_handle = torrent_loop(
        info_hash,
        dot_torrent_path,
        data_path,
        number_of_pieces,
        peer_id,
        port,
        left_bytes,
        max_peer_connections,
    )?;

    Ok((info_hash, torrent_handle))
}

fn torrent_loop(
    info_hash: InfoHash,
    dot_torrent_path: PathBuf,
    data_path: PathBuf,
    number_of_pieces: usize,
    peer_id: PeerId,
    port: u16,
    left_bytes: u64,
    max_peer_connections: Arc<Semaphore>,
) -> Result<TorrentHandle, Error> {
    let (torrent_tx, mut torrent_rx) = tokio::sync::mpsc::channel(20);

    let rt = tokio::runtime::Handle::current();

    let tx = torrent_tx.clone();

    let join_handle = rt.spawn(async move {
        let mut choke_timer = tokio::time::interval(std::time::Duration::from_secs(15));
        let mut announce_timer = tokio::time::interval(std::time::Duration::from_secs(300));
        let mut peer_connect_timer = tokio::time::interval(std::time::Duration::from_secs(15));

        let mut state = State {
            info_hash,
            dot_torrent_path,
            data_path,
            http_client: reqwest::Client::new(),
            pieces: bitvec![u8, Msb0; 0; number_of_pieces],
            connected_peers: Vec::new(),
            available_peers: HashSet::new(),
            max_peer_connections,
            peer_stats: BTreeMap::new(),
            last_announce: None,
            my_id: peer_id,
            port,
            left_bytes,
            uploaded_bytes: 0,
            downloaded_bytes: 0,
            state: TrackerState::Started,
            torrent_tx,
        };

        loop {
            select! {
                m = torrent_rx.recv() => {
                    let m = m.unwrap();
                    match m {
                        Message::GetState { reply_tx } => {
                            let _ = reply_tx.send(Ok(format!("{:?}", state)));
                        }
                        Message::Announce { reply_tx } => {
                            match state.announce().await {
                                Ok(response) => {
                                    state.handle_announce(&mut announce_timer, response.clone());
                                    let _ = reply_tx.send(Ok(response));
                                }
                                Err(e) => {
                                    let _ = reply_tx.send(Err(e));
                                }
                            }
                        }
                        Message::PeerDisconnection { peer_id } => {
                            let idx = state.connected_peers.iter().position(|peer| {
                                peer.remote_peer_id == peer_id
                            });
                            if let Some(idx) = idx {
                                let peer = state.connected_peers.remove(idx);
                                debug!("removed peer {:?} due to disconnection", peer.remote_peer_id);
                            }
                        }
                        Message::GetPieces { reply_tx } => {
                            let _ = reply_tx.send(Ok(state.pieces.clone()));
                        }
                        Message::VerifyLocalData => {
                            let _ = state.verify_local_data().await;
                        }
                    }
                }
                _ = peer_connect_timer.tick() => {
                    debug!("peer connect timer");
                    state.evaluate_and_connect_to_peers().await;
                }
                _ = announce_timer.tick() => {
                    match state.announce().await {
                        Ok(response) => {
                            state.handle_announce(&mut announce_timer, response);
                        }
                        Err(e) => {
                            println!("got error from tracker announce: {:#?}", e);
                        }
                    };
                }
                _ = choke_timer.tick() => {
                    println!("choke timer tick");
                }
            }
        }
    });

    Ok(TorrentHandle { tx, join_handle })
}

impl State {
    /// look at the peers we are currently connected to,
    /// look at what peers we have available,
    /// make some determination about what peers we should connect to,
    /// connect to those peers,
    /// disconnect from existing peers if necessary
    async fn evaluate_and_connect_to_peers(&mut self) {
        if self.max_peer_connections.available_permits() == 0 {
            for _ in 0..3 {
                let idx = rand::thread_rng().gen_range(0..self.connected_peers.len());

                if let Some(peer) = self.connected_peers.get(idx) {
                    peer.shutdown().await;
                };

                self.connected_peers.remove(idx);
            }
        }

        // find peers in self.available_peers that are not in self.connected peers.
        // maybe do this by set subtraction or something like that, based on ip address
        // TODO
        // let self.self.available_peers.
        let available_ips = self
            .available_peers
            .iter()
            .map(|peer| peer.ip)
            .collect::<HashSet<IpAddr>>();

        let connected_ips = self
            .connected_peers
            .iter()
            .map(|peer| peer.ip)
            .collect::<HashSet<IpAddr>>();

        for ip in available_ips.difference(&connected_ips) {
            debug!("ip: {}", ip);
            if let Some(peer) = self.available_peers.iter().find(|peer| peer.ip == *ip) {
                if let Some(peer_id) = peer.peer_id {
                    if peer_id == self.my_id {
                        continue;
                    }
                }

                debug!("connecting to peer {}, {:?}", peer.ip, peer.peer_id);
                if let Ok(peer_handle) = peer::new(
                    peer.ip,
                    peer.port,
                    self.info_hash,
                    self.my_id,
                    self.pieces.clone(),
                    peer.peer_id,
                    self.torrent_tx.clone(),
                    Arc::clone(&self.max_peer_connections),
                )
                .await
                {
                    self.connected_peers.push(peer_handle);
                };
            };
        }
    }

    fn handle_announce(
        &mut self,
        announce_timer: &mut Interval,
        response: Bencode,
    ) -> Option<ControlFlow<()>> {
        if let Some(failure_reason) = response.get("failure reason") {
            debug!("announce failure; failure_reason={:?}", failure_reason);
            return Some(ControlFlow::Continue(()));
        }

        if let Some(interval) = response.get("interval") {
            let interval = interval.as_u64();
            *announce_timer = tokio::time::interval(std::time::Duration::from_secs(interval));
            // must do this or else it fires immediately
            announce_timer.reset();
            debug!("announcing again in {} seconds", interval);
        }

        if let Some(peers) = response.get("peers") {
            let mut available_peers = HashSet::new();

            match peers {
                Bencode::List(l) => {
                    debug!("got {} peers from tracker", l.len());

                    for peer in l {
                        let peer_id = if let Some(peer_id) = peer.get("peer id") {
                            let peer_bytes = peer_id.as_bytes();
                            if peer_bytes.is_empty() {
                                None
                            } else {
                                let peer_bytes: [u8; 20] = peer_bytes.try_into().unwrap();
                                let peer_id = PeerId(peer_bytes);
                                Some(peer_id)
                            }
                        } else {
                            None
                        };

                        let ip = peer.get("ip").unwrap().as_str();

                        if let Ok(ip) = std::net::IpAddr::from_str(ip) {
                            let port = peer.get("port").unwrap().as_u16();

                            let available_peer = AvailablePeer { peer_id, ip, port };

                            if let Some(peer_id) = available_peer.peer_id {
                                if peer_id == self.my_id {
                                    continue;
                                }
                            }

                            available_peers.insert(available_peer);
                        } else {
                            continue;
                        }
                    }

                    debug!("got {} available peers on announce", available_peers.len());

                    self.available_peers = available_peers;
                }
                Bencode::ByteString(s) => todo!("process compact peers"),
                _ => panic!("TODO, bad peers response from tracker"),
            }
        }

        self.last_announce = Some(response.clone());

        None
    }

    async fn announce(&self) -> Result<Bencode, Error> {
        let mut buf = itoa::Buffer::new();
        let port = buf.format(self.port);

        let mut buf = itoa::Buffer::new();
        let left = buf.format(self.left_bytes);

        let mut buf = itoa::Buffer::new();
        let uploaded = buf.format(self.uploaded_bytes);

        let mut buf = itoa::Buffer::new();
        let downloaded = buf.format(self.downloaded_bytes);

        let urlencoded_info_hash = urlencoding::encode_binary(&self.info_hash.0);

        // TODO
        // something about how this is communicated over the wire is messed up
        let peer_id = urlencoding::encode_binary(&self.my_id.0);
        debug!("peer id in announce: {peer_id}");

        let event = self.state.to_string();

        let query_params = &[
            ("info_hash", urlencoded_info_hash),
            ("peer_id", peer_id),
            ("port", Cow::Borrowed(port)),
            ("left", Cow::Borrowed(left)),
            ("uploaded", Cow::Borrowed(uploaded)),
            ("downloaded", Cow::Borrowed(downloaded)),
            ("event", Cow::Owned(event)),
        ];

        // indescribably stupid that this is necessary because of
        // https://github.com/servo/rust-url/issues/219
        // and https://github.com/nox/serde_urlencoded/issues/44
        let mut announce_url = {
            let metainfos = METAINFOS.read().await;
            if let Some(metainfo) = metainfos.get(&self.info_hash) {
                metainfo.announce().to_string()
            } else {
                return Err(Error::NoMetainfo);
            }
        };

        announce_url.push('?');
        for (key, value) in query_params {
            announce_url.push_str(key);
            announce_url.push('=');
            announce_url.push_str(value);
            announce_url.push('&');
        }

        announce_url.pop();

        let response = self
            .http_client
            .get(announce_url)
            .send()
            .await?
            .bytes()
            .await?;

        Ok(bencode::decode(&response)?)
    }

    async fn verify_local_data(&mut self) -> Result<(), Error> {
        let piece_length = {
            if let Some(metainfo) = METAINFOS.read().await.get(&self.info_hash) {
                usize::try_from(metainfo.piece_length()).unwrap()
            } else {
                return Err(Error::NoMetainfo);
            }
        };

        let mut f = tokio::fs::File::open(&self.data_path).await?;
        // TODO parallelize

        let length = {
            if let Some(metainfo) = METAINFOS.read().await.get(&self.info_hash) {
                metainfo.length()
            } else {
                return Err(Error::NoMetainfo);
            }
        };

        let number_of_pieces = {
            if let Some(metainfo) = METAINFOS.read().await.get(&self.info_hash) {
                usize::try_from(metainfo.number_of_pieces()).unwrap()
            } else {
                return Err(Error::NoMetainfo);
            }
        };

        let nominal_length = number_of_pieces * piece_length;

        let left_over = nominal_length - usize::try_from(length).unwrap();
        let last_piece_length = piece_length - left_over;

        let mut normal_piece_buf = vec![0u8; piece_length];
        let mut last_piece_buf = vec![0u8; last_piece_length];

        if let Some(metainfo) = METAINFOS.read().await.get(&self.info_hash) {
            let piece_hashes = metainfo.piece_hashes_iter();

            for (i, piece_hash) in piece_hashes.enumerate() {
                f.seek(std::io::SeekFrom::Start(
                    u64::try_from(piece_length * i).unwrap(),
                ))
                .await?;

                let have_piece = if i == number_of_pieces - 1 {
                    f.read_exact(&mut last_piece_buf).await?;
                    &crate::hash(&last_piece_buf) == piece_hash
                } else {
                    f.read_exact(&mut normal_piece_buf).await?;
                    &crate::hash(&normal_piece_buf) == piece_hash
                };

                if have_piece {
                    self.pieces.set(i, true);
                } else {
                    self.pieces.set(i, false);
                }
            }

            Ok(())
        } else {
            Err(Error::NoMetainfo)
        }
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
struct AvailablePeer {
    peer_id: Option<PeerId>,
    ip: std::net::IpAddr,
    port: u16,
}

/// An opaque handle to a torrent that allows interaction with that torrent
/// via message passing. The actual torrent logic runs within its own task.
pub(crate) struct TorrentHandle {
    tx: tokio::sync::mpsc::Sender<Message>,
    join_handle: tokio::task::JoinHandle<()>,
}

impl TorrentHandle {
    /// The state of the torrent, dumped as a `Debug` string.
    pub(crate) async fn get_state_debug_string(&self) -> Result<String, Error> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.tx.send(Message::GetState { reply_tx }).await?;
        timeout!(reply_rx, 5).await??
    }

    /// Tell the torrent to announce to the tracker.
    /// May or may not actually announce, especially
    /// if the torrent has announced recently.
    pub(crate) async fn force_announce(&self) -> Result<Bencode, Error> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.tx.send(Message::Announce { reply_tx }).await?;
        timeout!(reply_rx, 5).await??
    }

    /// Get the pieces we have and don't have for this torrent as a bit vector
    pub(crate) async fn get_pieces(&self) -> Result<Pieces, Error> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.tx.send(Message::GetPieces { reply_tx }).await?;
        timeout!(reply_rx, 5).await??
    }

    /// Force a re-verification of the local torrent data.
    pub(crate) async fn verify_local_data(&self) -> Result<(), Error> {
        timeout!(self.tx.send(Message::VerifyLocalData), 5).await??;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // #[tokio::test]
    // async fn starts_up() {
    //     let t = Torrent::new(
    //         "a8dmfmt66t211.png.torrent",
    //         "/Users/clark/code/bib",
    //         Options::default(),
    //     )
    //     .await
    //     .unwrap();
    //     assert_eq!(
    //         t.get_info_hash().await.unwrap(),
    //         InfoHash([
    //             11, 146, 91, 1, 5, 86, 148, 10, 145, 237, 94, 213, 167, 245, 47, 79, 242, 9, 236,
    //             101
    //         ])
    //     );
    // }
}
