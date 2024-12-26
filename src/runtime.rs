use crate::timeout;
use crate::torrent::{self, InfoHash, Torrent, TorrentHandle};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio::task::JoinError;

#[derive(Debug, Error)]
pub enum Error {
    #[error("could not add torrent")]
    AddTorrent(String),
    #[error("unable to stop torrent")]
    StopTorrent(String),
    #[error("waiting for runtime")]
    Join(#[from] JoinError),
}

pub(crate) enum Message {
    AddTorrent {
        dot_torrent_path: PathBuf,
        data_path: PathBuf,
        options: torrent::Options,
        reply_tx: tokio::sync::oneshot::Sender<Result<InfoHash, torrent::Error>>,
    },
    StopTorrent {
        info_hash: InfoHash,
        reply_tx: tokio::sync::oneshot::Sender<Result<(), torrent::Error>>,
    },
}

pub struct Runtime {
    torrents: BTreeMap<InfoHash, TorrentHandle>,
}

#[derive(Clone, Debug, Default)]
pub struct Options {}

impl Runtime {
    pub fn new(options: Options) -> RuntimeHandle {
        let (tx, mut rx) = tokio::sync::mpsc::channel(100);

        let tokio_rt = tokio::runtime::Handle::current();

        let tx_clone = tx.clone();

        let t = tokio_rt.spawn(async move {
            let mut rt = Runtime {
                torrents: BTreeMap::new(),
            };

            loop {
                tokio::select! {
                    m = rx.recv() => {
                        let m = m.unwrap();
                        match m {
                            Message::AddTorrent{ dot_torrent_path, data_path, reply_tx, options }  => {
                                match Torrent::new(dot_torrent_path, data_path, tx_clone.clone(), options).await {
                                    Ok(torrent) => {
                                        let info_hash = torrent.get_info_hash().await.unwrap();
                                        rt.torrents.insert(info_hash, torrent);
                                        let _ = reply_tx.send(Ok(info_hash));
                                    },
                                    Err(e) => {
                                        let _ = reply_tx.send(Err(e));
                                    },
                                };
                            },
                            Message::StopTorrent { info_hash, reply_tx } => {
                                if let Some(torrent) = rt.torrents.get(&info_hash) {
                                    let stop_result = torrent.stop().await;
                                    let _ = reply_tx.send(stop_result);
                                } else {
                                    let _ = reply_tx.send(Ok(()));
                                }
                            }
                        }
                    }
                }
            }
        });

        RuntimeHandle { tx, t }
    }
}

pub struct RuntimeHandle {
    tx: tokio::sync::mpsc::Sender<Message>,
    t: tokio::task::JoinHandle<()>,
}

impl RuntimeHandle {
    pub async fn block_on(self) -> Result<(), Error> {
        self.t.await?;
        Ok(())
    }

    pub async fn add_torrent<P: ?Sized + AsRef<Path>>(
        &self,
        dot_torrent_path: &P,
        data_path: &P,
        options: torrent::Options,
    ) -> Result<InfoHash, Error> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(Message::AddTorrent {
                dot_torrent_path: dot_torrent_path.as_ref().to_owned(),
                data_path: data_path.as_ref().to_owned(),
                options,
                reply_tx,
            })
            .await
            .map_err(|e| Error::AddTorrent(e.to_string()))?;

        reply_rx
            .await
            .map_err(|e| Error::AddTorrent(e.to_string()))?
            .map_err(|e| Error::AddTorrent(e.to_string()))
    }

    async fn stop_torrent(&self, info_hash: InfoHash) -> Result<(), Error> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();

        timeout!(
            self.tx.send(Message::StopTorrent {
                info_hash,
                reply_tx
            }),
            5
        )
        .await
        .map_err(|e| Error::StopTorrent(e.to_string()))?
        .map_err(|e| Error::StopTorrent(e.to_string()))?;

        // awful
        timeout!(reply_rx, 5)
            .await
            .map_err(|e| Error::StopTorrent(e.to_string()))?
            .map_err(|e| Error::StopTorrent(e.to_string()))?
            .map_err(|e| Error::StopTorrent(e.to_string()))?;

        Ok(())
    }
}
