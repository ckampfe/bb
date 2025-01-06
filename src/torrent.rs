use crate::bencode::{self, Bencode};
use crate::metainfo::MetaInfo;
use crate::peer::{self, PeerHandle};
use crate::{download, timeout, InfoHash, PeerId, Port};
use bitvec::prelude::*;
use bitvec::vec::BitVec;
use rand::Rng;
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt::{Debug, Display};
use std::net::IpAddr;
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::select;
use tokio::sync::{broadcast, mpsc, oneshot, RwLock, Semaphore};
use tokio::time::Interval;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, info};

pub type Pieces = BitVec<u8, bitvec::order::Msb0>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("could not read .torrent file")]
    DotTorrentRead(String),
    #[error("bencode error")]
    Bencode(#[from] bencode::Error),
    #[error("oneshot channel error")]
    OneshotChannelRecv(#[from] oneshot::error::RecvError),
    #[error("coudl not send on channel")]
    MpscSend(#[from] mpsc::error::SendError<AllToTorrentMessage>),
    #[error("timeout")]
    Timeout(#[from] tokio::time::error::Elapsed),
    #[error("announce")]
    Announce(#[from] reqwest::Error),
    #[error("io")]
    Io(#[from] std::io::Error),
    #[error("the metainfo no longer exists for some reasons")]
    NoMetainfo,
    #[error("unable to get pieces for this torrent")]
    NoPieces,
    #[error("ldfjdf")]
    InternalBroadcast(#[from] broadcast::error::SendError<GossipMessage>),
}

/// used to send messages to the torrent only
pub enum AllToTorrentMessage {
    GetState {
        reply_tx: tokio::sync::oneshot::Sender<Result<String, Error>>,
    },
    Announce {
        reply_tx: tokio::sync::oneshot::Sender<Result<Bencode, Error>>,
    },
    PeerDisconnection {
        peer_id: PeerId,
    },
    VerifyLocalData,
    GetDataLocation {
        reply_tx: tokio::sync::oneshot::Sender<Result<PathBuf, Error>>,
    },
    PeerUploadedToUs {
        peer_id: PeerId,
    },
    PeerDownloadedFromUs {
        peer_id: PeerId,
    },
    GetPeerArgs {
        reply_tx: oneshot::Sender<PeerArgs>,
    },
    AddIncomingPeer { peer_handle: PeerHandle }
}

pub(crate) struct PeerArgs {
    pub pieces: Arc<RwLock<Pieces>>,
    pub info_hash: InfoHash,
    pub metainfo: Arc<MetaInfo>,
    pub gossip_tx: broadcast::Sender<GossipMessage>,
    pub gossip_rx: broadcast::Receiver<GossipMessage>,
    pub my_id: PeerId,
    pub data_path: PathBuf,
    pub torrent_tx: mpsc::Sender<AllToTorrentMessage>,
    pub task_tracker: TaskTracker,
    pub cancellation_token: CancellationToken,
    pub max_torrent_connections: Arc<Semaphore>,
    pub global_max_connections: Arc<Semaphore>,
}

/// used in 2 scenarios:
/// - torrent to send message to all peers
/// - peer to send message to all other peers AND to torrent
#[derive(Clone, Copy, Debug)]
pub enum GossipMessage {
    /// peers broadcast this message to let the other peers in the cluster know
    /// that they have a message.
    /// named this way to differentiate it from `Frame::Have`,
    /// to indicate that we are the ones who now have a piece, not a remote peer.
    WeHave {
        index: u32,
    },
    DownloadComplete,
    Choke,
}

/// the state of a single running torrent
#[derive(Debug)]
struct State {
    /// the channel to communicate to the runtime itself, if need be
    info_hash: InfoHash,
    /// location of the .torrent
    dot_torrent_path: PathBuf,
    metainfo: Arc<MetaInfo>,
    /// the current progress of the torrent, which pieces we have and don't have.
    /// all peers receive a clone of this Arc, and update it themselves
    /// as they download pieces
    pieces: Arc<RwLock<Pieces>>,
    /// download location
    data_path: PathBuf,
    /// for tracker announce/scrape/etc
    http_client: reqwest::Client,
    /// peer's we're connected to
    connected_peers: Vec<PeerHandle>,
    /// peers, as we get them from the tracker
    /// a hashet ensures that we only ever have one entry per unique peer
    available_peers: HashSet<AvailablePeer>,
    /// how much each individual peer has uploaded and downloaded
    /// TODO actually use this for the choke algorithm
    peer_stats: BTreeMap<PeerId, PeerStats>,
    /// the last announce we've received from the tracker
    last_announce: Option<Bencode>,
    /// our unique id. TODO is this unique per torrent or per client?
    my_id: PeerId,
    /// the port we are listening on
    /// TODO why do we store this here instead of getting it from the client?
    port: u16,
    /// the number of bytes remaining to download for this torrent to be complete
    left_bytes: u64,
    /// the number of bytes we have uploaded over the course of this torrent's life.
    /// TODO figure out some way to track this persistently
    uploaded_bytes: u64,
    /// the number of bytes we have downloaded over the course of this torrent's life.
    /// TODO figure out some way to track this persistently
    downloaded_bytes: u64,
    /// the logical state we are in, whether we are seeding or leaching or something else
    state: TorrentState,
    /// the state we report to the tracker
    tracker_state: TrackerState,
    /// used to spawn and ensure all peer tasks for this torrent properly shut down
    /// when commanded to do so
    task_tracker: TaskTracker,
    /// used to command peer tasks to shut down
    cancellation_token: CancellationToken,
    /// the number of max connections allowed within the context of this torrent only
    max_peer_connections: Arc<Semaphore>,
    /// the global number of connections allowed across the entire client
    global_max_connections: Arc<Semaphore>,
    /// the way that a peer or client can send a message to this torrent task
    torrent_tx: mpsc::Sender<AllToTorrentMessage>,
    /// the way that the peers and torrent communicate with each other. omnidirectional.
    gossip_tx: broadcast::Sender<GossipMessage>,
}

#[derive(Debug, PartialEq)]
enum TorrentState {
    Seeding,
    Leaching,
}

/// stats for a single peer
#[derive(Debug)]
struct PeerStats {
    blocks_uploaded_to_us: usize,
    blocks_downloaded_from_us: usize,
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
    max_connections: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            port: Port::Port(6881),
            max_connections: 40,
        }
    }
}

pub(crate) async fn new<P: AsRef<Path>>(
    dot_torrent_path: P,
    data_path: P,
    global_max_connections: Arc<Semaphore>,
    task_tracker: TaskTracker,
    cancellation_token: CancellationToken,
    options: Options,
) -> Result<(crate::InfoHash, TorrentHandle), Error> {
    let dot_torrent_data = tokio::fs::read(&dot_torrent_path)
        .await
        .map_err(|e| Error::DotTorrentRead(e.to_string()))?;
    let dot_torrent_decoded = bencode::decode(&dot_torrent_data).unwrap();
    let metainfo = MetaInfo::new(dot_torrent_decoded);
    let len = metainfo.length();
    let mut full_file_path = data_path.as_ref().to_owned();
    full_file_path.push(metainfo.name());
    let info_hash = metainfo.info_hash()?;
    let number_of_pieces = usize::try_from(metainfo.number_of_pieces()).unwrap();
    // TODO actually calculate the number of bytes left
    let left_bytes = metainfo.length();

    let dot_torrent_path = dot_torrent_path.as_ref().to_path_buf();
    let data_path = data_path.as_ref().to_path_buf();

    // TODO create file if it doesn't exist
    let f = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(full_file_path)
        .await?;

    f.set_len(len).await?;

    // TODO randomize this in release
    let mut peer_id_bytes = [0u8; 20];
    // rand::thread_rng().fill_bytes(&mut peer_id_bytes);
    let peer_id = PeerId(peer_id_bytes);

    let port = match options.port {
        Port::Port(port) => port,
        Port::Random => rand::thread_rng().gen::<u16>(),
    };

    let max_peer_connections = Arc::new(Semaphore::new(options.max_connections));

    let torrent_handle = torrent_loop(
        info_hash,
        metainfo,
        dot_torrent_path,
        data_path,
        number_of_pieces,
        peer_id,
        port,
        left_bytes,
        task_tracker,
        cancellation_token,
        max_peer_connections,
        global_max_connections,
    )?;

    Ok((info_hash, torrent_handle))
}

fn torrent_loop(
    info_hash: InfoHash,
    metainfo: MetaInfo,
    dot_torrent_path: PathBuf,
    data_path: PathBuf,
    number_of_pieces: usize,
    peer_id: PeerId,
    port: u16,
    left_bytes: u64,
    task_tracker: TaskTracker,
    cancellation_token: CancellationToken,
    max_peer_connections: Arc<Semaphore>,
    global_max_connections: Arc<Semaphore>,
) -> Result<TorrentHandle, Error> {
    let (torrent_tx, mut torrent_rx) = mpsc::channel(20);

    let tx = torrent_tx.clone();

    let (gossip_tx, mut gossip_rx) = broadcast::channel(20);

    let task_tracker_clone = task_tracker.clone();

    let join_handle = task_tracker.spawn(async move {
        let mut choke_timer = tokio::time::interval(std::time::Duration::from_secs(15));
        let mut announce_timer = tokio::time::interval(std::time::Duration::from_secs(300));
        let mut peer_connect_timer = tokio::time::interval(std::time::Duration::from_secs(15));

        let mut state = State {
            info_hash,
            metainfo: Arc::new(metainfo),
            pieces: Arc::new(RwLock::new(bitvec![u8, Msb0; 0; number_of_pieces])),
            dot_torrent_path,
            data_path,
            http_client: reqwest::Client::new(),
            connected_peers: Vec::new(),
            available_peers: HashSet::new(),
            max_peer_connections,
            global_max_connections,
            peer_stats: BTreeMap::new(),
            last_announce: None,
            my_id: peer_id,
            port,
            left_bytes,
            uploaded_bytes: 0,
            downloaded_bytes: 0,
            state: TorrentState::Leaching,
            tracker_state: TrackerState::Started,
            torrent_tx,
            gossip_tx,
            task_tracker: task_tracker_clone.clone(),
            cancellation_token: cancellation_token.clone()
        };

        state.verify_local_data().await?;

        loop {
            select! {
                _ = state.cancellation_token.cancelled() => {
                    debug!("{:?} shutting down torrent task", state.info_hash);
                    break
                }
                Ok(m) = gossip_rx.recv() => {
                    match m {
                        // every time a peer reports that we now have a new piece,
                        // check if we are done.
                        // peers update the pieces directly,
                        // so we don't have to set them here.
                        GossipMessage::WeHave { .. } => {
                            let my_pieces = state.pieces.read().await;

                            if my_pieces.all() {
                                // we are done!
                                info!("download complete {:?}", info_hash);

                                state.gossip_tx.send(GossipMessage::DownloadComplete)?;

                                state.state = TorrentState::Seeding;
                                state.tracker_state = TrackerState::Completed;

                                state.announce().await?;
                            }
                        },
                        GossipMessage::DownloadComplete => {}
                        GossipMessage::Choke => {}
                    }
                }
                m = torrent_rx.recv() => {
                    let m = m.unwrap();
                    match m {
                        AllToTorrentMessage::AddIncomingPeer { peer_handle } => {
                            state.connected_peers.push(peer_handle);
                        }
                        AllToTorrentMessage::GetPeerArgs { reply_tx } => {
                            let _ = reply_tx.send(PeerArgs { 
                                pieces: Arc::clone(&state.pieces),
                                info_hash,
                                metainfo: Arc::clone(&state.metainfo),
                                gossip_rx: state.gossip_tx.subscribe(),
                                gossip_tx: state.gossip_tx.clone(),
                                my_id: state.my_id,
                                data_path: state.data_path.clone(),
                                torrent_tx: state.torrent_tx.clone(),
                                task_tracker: task_tracker_clone.clone(),
                                cancellation_token: state.cancellation_token.clone(),
                                max_torrent_connections: Arc::clone(&state.max_peer_connections),
                                global_max_connections: Arc::clone(&state.global_max_connections)
                            });
                        }
                        AllToTorrentMessage::GetState { reply_tx } => {
                            let _ = reply_tx.send(Ok(format!("{:?}", state)));
                        }
                        AllToTorrentMessage::Announce { reply_tx } => {
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
                        // TODO get rid of this in favor of a global PIECES
                        // infohash->pieces map,
                        // to save memory so we're not just copying around a ton of
                        // pieces bitsets to dozens or potential hundreds of peer tasks,
                        // and having to keep them in sync.
                        // just have one, and keep it in sync, since it's "global" to that
                        // particular torrent anyway
                        // Message::Have { index } => {
                        //     // state.pieces.set(index as usize, true);
                        //     if let Some(pieces) = PIECES.write().get(state.info_hash) {
                        //         todo!()
                        //     }
                        // }
                        AllToTorrentMessage::PeerDisconnection { peer_id } => {
                            let idx = state.connected_peers.iter().position(|peer| {
                                peer.remote_peer_id == peer_id
                            });
                            if let Some(idx) = idx {
                                let peer = state.connected_peers.remove(idx);
                                debug!("removed peer {:?} due to disconnection", peer.remote_peer_id);
                            }
                        }
                        AllToTorrentMessage::VerifyLocalData => {
                            let _ = state.verify_local_data().await;
                        }
                        AllToTorrentMessage::GetDataLocation { reply_tx } => {
                            let _ = reply_tx.send(Ok(state.data_path.clone()));
                        }
                        AllToTorrentMessage::PeerUploadedToUs { peer_id } => {
                            let stats = state.peer_stats.entry(peer_id).or_insert(PeerStats { blocks_uploaded_to_us: 0, blocks_downloaded_from_us: 0 });
                            stats.blocks_uploaded_to_us += 1;
                        }
                        AllToTorrentMessage::PeerDownloadedFromUs { peer_id } => {
                            let stats = state.peer_stats.entry(peer_id).or_insert(PeerStats { blocks_uploaded_to_us: 0, blocks_downloaded_from_us: 0 });
                            stats.blocks_downloaded_from_us += 1;
                        }
                    }
                }
                _ = peer_connect_timer.tick() => {
                    debug!("peer connect timer");
                    if state.state != TorrentState::Seeding {
                        state.evaluate_and_connect_to_peers().await;
                    }
                }
                _ = announce_timer.tick() => {
                    match state.announce().await {
                        Ok(response) => {
                            state.handle_announce(&mut announce_timer, response);
                        }
                        Err(e) => {
                            debug!("got error from tracker announce: {:#?}", e);
                        }
                    };
                }
                _ = choke_timer.tick() => {
                    debug!("choke timer tick");
                    state.handle_choke_timer().await?;
                }
            }
        }

        Ok(())
    });

    Ok(TorrentHandle { tx, join_handle })
}

impl State {
    async fn handle_choke_timer(&mut self)-> Result<(), Error> {
        // TODO actually assess peer stats and do something about it
        // do work here to assess and unchoke
        // 1. unchoke 4 peers with best ratio that are interested.
        // and reset.
        self.peer_stats.clear();

        // send choke signal to all peers.
        // peers will only send the choke message to the remote peer
        // if they need to.
        // for peer in self.connected_peers.iter() {
        //     peer.choke().await;
        // }
        self.gossip_tx.send(GossipMessage::Choke)?;
        debug!("told all peers to be choked");

        let peers_to_unchoke = self.random_peers_to_unchoke(4);

        for peer in peers_to_unchoke {
            debug!("unchoking {:?}", peer.remote_peer_id);
            peer.unchoke().await;
        }

        Ok(())
    }

    // if peers.len() >= n, returns n peers,
    // otherwise returns peers.len() peers
    fn random_peers_to_unchoke(&self, n: usize) -> Vec<&PeerHandle> {
        let mut peers = vec![];

        let mut idxs = BTreeSet::new();

        while idxs.len() < n && idxs.len() < self.connected_peers.len() {
            let idx = rand::thread_rng().gen_range(0..self.connected_peers.len());
            idxs.insert(idx);
        }

        for idx in idxs {
            if let Some(peer) = self.connected_peers.get(idx) {
                peers.push(peer);
            };
        }

        peers
    }


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
                    Arc::clone(&self.pieces),
                    self.gossip_tx.clone(),
                    self.gossip_tx.subscribe(),
                    &self.data_path,
                    peer.ip,
                    peer.port,
                    self.info_hash,
                    Arc::clone(&self.metainfo),
                    self.my_id,
                    // self.pieces.clone(),
                    peer.peer_id,
                    self.torrent_tx.clone(),
                    self.task_tracker.clone(),
                    self.cancellation_token.clone(),
                    Arc::clone(&self.max_peer_connections),
                    Arc::clone(&self.global_max_connections),
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

        let event = self.tracker_state.to_string();

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
        // let mut announce_url = {
        //     let metainfos = METAINFOS.read().await;
        //     if let Some(metainfo) = metainfos.get(&self.info_hash) {
        //         metainfo.announce().to_string()
        //     } else {
        //         return Err(Error::NoMetainfo);
        //     }
        // };
        // let announce_url = self.metainfo.announce().to_string();
        let mut announce_url = self.metainfo.announce().to_string();

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
        let piece_length = usize::try_from(self.metainfo.piece_length()).unwrap();

        let mut to_read = self.data_path.clone();
        to_read.push(self.metainfo.name());
        let mut f = tokio::fs::File::open(to_read).await?;
        debug!("here1");
        // TODO parallelize

        let length = self.metainfo.length();

        let number_of_pieces = usize::try_from(self.metainfo.number_of_pieces()).unwrap();

        let nominal_length = number_of_pieces * piece_length;

        let left_over = nominal_length - usize::try_from(length).unwrap();
        let last_piece_length = piece_length - left_over;

        let mut normal_piece_buf = vec![0u8; piece_length];
        let mut last_piece_buf = vec![0u8; last_piece_length];

        let piece_hashes = self.metainfo.piece_hashes_iter();

        for (i, piece_hash) in piece_hashes.enumerate() {
            debug!("here3");
            f.seek(std::io::SeekFrom::Start(
                u64::try_from(piece_length * i).unwrap(),
            ))
            .await?;


            // let have_piece = if i == number_of_pieces - 1 {
            //     debug!("here5");
            //     f.read_exact(&mut last_piece_buf).await?;
            //     &crate::hash(&last_piece_buf) == piece_hash
            // } else {
            //     debug!("here4");
            //     f.read_exact(&mut normal_piece_buf).await?;
            //     &crate::hash(&normal_piece_buf) == piece_hash
            // };
            let have_piece = download::verify_piece(&self.metainfo, &mut f, i.try_into().unwrap()).await?;
            dbg!(i);
            dbg!(have_piece);

            if have_piece {
                let mut my_pieces = self.pieces.write().await;
                my_pieces.set(i, true);
            } else {
                let mut my_pieces = self.pieces.write().await;
                my_pieces.set(i, false);
            }
        }
        debug!("here2");

        let my_pieces = self.pieces.read().await;
        let have = my_pieces.count_ones();
        let not_have = my_pieces.count_zeros();

        if my_pieces.all() {
            self.state = TorrentState::Seeding;
            // we don't set completed here because we probably already announced we were completed,
            // and we don't need to do it again
            self.tracker_state = TrackerState::Started;
        }

        debug!("verified local data, have: {}, not have: {}", have, not_have);


        Ok(())
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
    tx: tokio::sync::mpsc::Sender<AllToTorrentMessage>,
    join_handle: tokio::task::JoinHandle<Result<(), Error>>,
}

impl TorrentHandle {
    pub(crate) async fn get_peer_args(&self) -> Result<PeerArgs, Error> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(AllToTorrentMessage::GetPeerArgs { reply_tx })
            .await?;
        Ok(timeout!(reply_rx, 5).await??)
    }

    /// The state of the torrent, dumped as a `Debug` string.
    pub(crate) async fn get_state_debug_string(&self) -> Result<String, Error> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(AllToTorrentMessage::GetState { reply_tx })
            .await?;
        timeout!(reply_rx, 5).await??
    }

    /// Tell the torrent to announce to the tracker.
    /// May or may not actually announce, especially
    /// if the torrent has announced recently.
    pub(crate) async fn force_announce(&self) -> Result<Bencode, Error> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(AllToTorrentMessage::Announce { reply_tx })
            .await?;
        timeout!(reply_rx, 5).await??
    }

    /// Force a re-verification of the local torrent data.
    pub(crate) async fn verify_local_data(&self) -> Result<(), Error> {
        timeout!(self.tx.send(AllToTorrentMessage::VerifyLocalData), 5).await??;
        Ok(())
    }

    pub(crate) async fn get_data_location(&self) -> Result<PathBuf, Error> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        timeout!(
            self.tx
                .send(AllToTorrentMessage::GetDataLocation { reply_tx }),
            5
        )
        .await??;
        timeout!(reply_rx, 5).await??
    }
    
    pub(crate) async fn add_incoming_peer(&self, peer_handle: PeerHandle) -> Result<(), Error> {
        timeout!(self.tx.send(AllToTorrentMessage::AddIncomingPeer { peer_handle }), 5).await??;
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
