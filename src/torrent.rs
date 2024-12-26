use crate::bencode::{self, Bencode};
use crate::metainfo::MetaInfo;
use crate::peer::PeerHandle;
use crate::timeout;
use base64::Engine;
use bitvec::prelude::*;
use bitvec::vec::BitVec;
use rand::{Rng, RngCore};
use std::borrow::Cow;
use std::collections::{BTreeMap, HashSet};
use std::fmt::{Debug, Display};
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::select;
use tokio::time::Interval;
use tracing::debug;

pub type Pieces = BitVec<u8, bitvec::order::Msb0>;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct InfoHash(pub [u8; 20]);

impl Debug for InfoHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let base64 = base64::prelude::BASE64_STANDARD.encode(self.0);
        write!(f, "{}", base64)
    }
}

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
    #[error("coudl not send on chnnael")]
    MpscSend(#[from] tokio::sync::mpsc::error::SendError<Message>),
    #[error("timeout")]
    Timeout(#[from] tokio::time::error::Elapsed),
    #[error("announce")]
    Announce(#[from] reqwest::Error),
    #[error("io")]
    Io(#[from] std::io::Error),
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
    VerifyLocalData,
    Stop {
        reply_tx: tokio::sync::oneshot::Sender<()>,
    },
}

#[derive(Debug)]
pub(crate) struct TorrentState {
    /// the channel to communicate to the runtime itself, if need be
    // rt_tx: tokio::sync::mpsc::Sender<runtime::Message>,
    /// the metainfo parsed from the .torrent file
    metainfo: MetaInfo,
    /// location of the .torrent
    dot_torrent_path: PathBuf,
    /// download location
    data_path: PathBuf,
    /// for tracker announce/scrape/etc
    http_client: reqwest::Client,
    /// the pieces of the torrent that we have
    pieces: Pieces,
    /// peer's we're connected to
    connected_peers: BTreeMap<PeerId, PeerHandle>,
    /// peers, as we get them from the tracker
    /// a hashet ensures that we only ever have one entry per unique peer
    available_peers: HashSet<AvailablePeer>,
    /// the last announce we've received from the tracker
    last_announce: Option<Bencode>,
    // ("peer_id", self.peer_id),
    peer_id: PeerId,
    // ("port", self.port),
    port: u16,
    // ("left", self.left),
    left_bytes: u64,
    // ("uploaded", self.uploaded),
    uploaded_bytes: u64,
    // ("downloaded", self.downloaded),
    downloaded_bytes: u64,
    // ("event", self.state),
    state: State,
}

#[derive(Debug)]
enum State {
    Started,
    Stopped,
    Completed,
}

impl Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let out = match self {
            State::Started => "started",
            State::Stopped => "stopped",
            State::Completed => "completed",
        };

        write!(f, "{}", out)
    }
}

pub struct Options {
    port: Port,
}

impl Default for Options {
    fn default() -> Self {
        Self { port: Port::Random }
    }
}

enum Port {
    Port(u16),
    Random,
}

impl TorrentState {
    pub(crate) async fn new<P: AsRef<Path>>(
        dot_torrent_path: P,
        data_path: P,
        // rt_tx: tokio::sync::mpsc::Sender<runtime::Message>,
        options: Options,
    ) -> Result<(InfoHash, TorrentHandle), Error> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(20);

        let dot_torrent_data = tokio::fs::read(&dot_torrent_path)
            .await
            .map_err(|e| Error::DotTorrentRead(e.to_string()))?;
        let dot_torrent_decoded = bencode::decode(&dot_torrent_data).unwrap();
        let metainfo = MetaInfo::new(dot_torrent_decoded);
        let info_hash = metainfo.info_hash()?;
        let number_of_pieces = usize::try_from(metainfo.number_of_pieces()).unwrap();

        let dot_torrent_path = dot_torrent_path.as_ref().to_path_buf();
        let data_path = data_path.as_ref().to_path_buf();

        // TODO create file if it doesn't exist

        let mut peer_id_bytes = [0u8; 20];
        rand::thread_rng().fill_bytes(&mut peer_id_bytes);
        let peer_id = PeerId(peer_id_bytes);

        let port = match options.port {
            Port::Port(port) => port,
            Port::Random => rand::thread_rng().gen::<u16>(),
        };

        let rt = tokio::runtime::Handle::current();

        let join_handle = rt.spawn(async move {
            let mut choke_timer = tokio::time::interval(std::time::Duration::from_secs(15));
            let mut announce_timer = tokio::time::interval(std::time::Duration::from_secs(300));

            let left_bytes = metainfo.length();

            let mut state = TorrentState {
                // rt_tx,
                metainfo,
                dot_torrent_path,
                data_path,
                http_client: reqwest::Client::new(),
                pieces: bitvec![u8, Msb0; 0; number_of_pieces],
                connected_peers: BTreeMap::new(),
                available_peers: HashSet::new(),
                last_announce: None,
                peer_id,
                port,
                left_bytes,
                uploaded_bytes: 0,
                downloaded_bytes: 0,
                state: State::Started,
            };

            loop {
                select! {
                    m = rx.recv() => {
                        let m = m.unwrap();
                        match m {
                            Message::GetState { reply_tx } => {
                                let _ = reply_tx.send(Ok(format!("{:?}", state)));
                            }
                            Message::Announce { reply_tx } => {
                                match state.announce().await {
                                    Ok(response) => {
                                        state.handle_announce(&mut announce_timer, response.clone());
                                        state.connect_to_peers().await;
                                        let _ = reply_tx.send(Ok(response));
                                    }
                                    Err(e) => {
                                        let _ = reply_tx.send(Err(e));
                                    }
                                }
                            }
                            Message::GetPieces { reply_tx } => {
                                let _ = reply_tx.send(Ok(state.pieces.clone()));
                            }
                            Message::VerifyLocalData => {
                                let _ = state.verify_local_data().await;
                            }
                            Message::Stop { reply_tx } => {
                                reply_tx.send(()).unwrap();
                                break;
                            }
                        }
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

        Ok((info_hash, TorrentHandle { tx, join_handle }))
    }

    async fn connect_to_peers(&self) -> Result<(), Error> {
        todo!()
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
            let interval = interval.as_str();
            let interval: u64 = interval.parse().unwrap();
            *announce_timer = tokio::time::interval(std::time::Duration::from_secs(interval));
        }

        if let Some(peers) = response.get("peers") {
            match peers {
                Bencode::List(l) => {
                    debug!("got {} peers from tracker", l.len());

                    for peer in l {
                        let peer_id =
                            PeerId(peer.get("peer id").unwrap().as_bytes().try_into().unwrap());
                        let ip = peer.get("ip").unwrap().as_str().to_string();
                        let port = peer.get("port").unwrap().as_u16();
                        let available_peer = AvailablePeer {
                            peer_id: Some(peer_id),
                            ip,
                            port,
                        };

                        self.available_peers.insert(available_peer);
                    }
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

        let info_hash = &self.metainfo.info_hash()?.0;
        let info_hash = urlencoding::encode_binary(info_hash);

        let peer_id = urlencoding::encode_binary(&self.peer_id.0);

        let event = self.state.to_string();

        let query_params = &[
            ("info_hash", info_hash),
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
        let mut f = tokio::fs::File::open(&self.data_path).await?;
        // TODO parallelize

        let length = self.metainfo.length();
        let number_of_pieces = usize::try_from(self.metainfo.number_of_pieces()).unwrap();
        let nominal_length = number_of_pieces * piece_length;

        let left_over = nominal_length - usize::try_from(length).unwrap();
        let last_piece_length = piece_length - left_over;

        let mut normal_piece_buf = vec![0u8; piece_length];
        let mut last_piece_buf = vec![0u8; last_piece_length];

        for (i, piece_hash) in self.metainfo.piece_hashes_iter().enumerate() {
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
    }
}

// https://github.com/nox/serde_urlencoded/pull/60
// fn encode_into(
//     input: impl serde::ser::Serialize,
//     encoding: impl Fn(&str) -> Cow<[u8]>,
// ) -> Result<String, serde_urlencoded::ser::Error> {
//     let mut urlencoder = form_urlencoded::Serializer::new("".to_owned());
//     urlencoder.encoding_override(Some(&encoding));
//     input.serialize(serde_urlencoded::Serializer::new(&mut urlencoder))?;
//     Ok(urlencoder.finish())
// }

#[derive(Debug, Hash, PartialEq, Eq)]
struct AvailablePeer {
    peer_id: Option<PeerId>,
    ip: String,
    port: u16,
}

pub(crate) struct TorrentHandle {
    tx: tokio::sync::mpsc::Sender<Message>,
    join_handle: tokio::task::JoinHandle<()>,
}

impl TorrentHandle {
    pub(crate) async fn get_state(&self) -> Result<String, Error> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.tx.send(Message::GetState { reply_tx }).await?;
        timeout!(reply_rx, 5).await??
    }

    pub(crate) async fn announce(&self) -> Result<Bencode, Error> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.tx.send(Message::Announce { reply_tx }).await?;
        timeout!(reply_rx, 5).await??
    }

    pub(crate) async fn get_pieces(&self) -> Result<Pieces, Error> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.tx.send(Message::GetPieces { reply_tx }).await?;
        timeout!(reply_rx, 5).await??
    }

    pub(crate) async fn verify_local_data(&self) -> Result<(), Error> {
        timeout!(self.tx.send(Message::VerifyLocalData), 5).await??;
        Ok(())
    }

    pub(crate) async fn stop(&self) -> Result<(), Error> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.tx.send(Message::Stop { reply_tx }).await?;
        Ok(timeout!(reply_rx, 5).await??)
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