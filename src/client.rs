use crate::torrent::{self, TorrentHandle};
use crate::{peer, Error, InfoHash, Port};
use rand::Rng;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::debug;

/// global options
pub struct Options {
    pub listen_port: Port,
    pub max_connections: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            listen_port: Port::Port(6881),
            max_connections: 200,
        }
    }
}

// should this be one pointer to an inner struct?
#[derive(Clone)]
pub struct Client {
    /// the way to access torrents to issue them commands
    torrents: Arc<RwLock<BTreeMap<InfoHash, TorrentHandle>>>,
    /// the maximum number of connections the client will allow
    global_max_connections: Arc<tokio::sync::Semaphore>,
    /// after issuing the cancellation token,
    /// the task tracker gives us a future we can await
    /// to know when tasks are completely shut down
    task_tracker: TaskTracker,
    /// every subtask spawned for either a torrent or a peer connection
    /// receives this cancellation token,
    /// which is run when the client is dropped.
    cancellation_token: CancellationToken,
}

impl Client {
    pub fn new(options: Options) -> Self {
        let task_tracker = TaskTracker::new();

        let port = match options.listen_port {
            Port::Port(p) => p,
            Port::Random => rand::rng().random(),
        };

        let torrents: Arc<RwLock<BTreeMap<InfoHash, TorrentHandle>>> =
            Arc::new(RwLock::new(BTreeMap::new()));

        let torrents_for_listener = Arc::clone(&torrents);

        let task_tracker_for_listener = task_tracker.clone();

        task_tracker.spawn(async move {
            let torrents = torrents_for_listener;

            let task_tracker = task_tracker_for_listener;

            let listener = tokio::net::TcpListener::bind(("::1", port)).await.unwrap();

            loop {
                let (mut socket, _addr) = listener.accept().await.unwrap();

                debug!(
                    "accepted connection from {:?}",
                    socket.peer_addr().unwrap().ip()
                );

                let torrents_for_prospective_peer = Arc::clone(&torrents);

                task_tracker.spawn(async move {
                    let torrents = torrents_for_prospective_peer;

                    let handshake_frame = peer::receive_handshake(&mut socket).await.unwrap();

                    debug!(
                        "received handshake from incoming peer {:?}",
                        handshake_frame.peer_id
                    );

                    let torrents = torrents.read().await;

                    if let Some(torrent) = torrents.get(&handshake_frame.info_hash) {
                        let peer_args = torrent.get_peer_args().await.unwrap();

                        let peer_handle =
                            peer::from_socket(socket, handshake_frame.peer_id, peer_args)
                                .await
                                .unwrap();

                        debug!("got peer handle for {:?}", peer_handle.remote_peer_id);

                        torrent.add_incoming_peer(peer_handle).await.unwrap();

                        debug!("added incoming peer to torrent")
                    }
                });
            }
        });

        Self {
            torrents,
            global_max_connections: Arc::new(tokio::sync::Semaphore::new(options.max_connections)),
            task_tracker,
            cancellation_token: CancellationToken::new(),
        }
    }

    /// add and start a new torrent
    pub async fn new_torrent<P: AsRef<Path>>(
        &self,
        dot_torrent_path: P,
        data_path: P,
        options: torrent::Options,
    ) -> Result<InfoHash, Error> {
        let (info_hash, torrent_handle) = torrent::new(
            dot_torrent_path,
            data_path,
            Arc::clone(&self.global_max_connections),
            self.task_tracker.clone(),
            self.cancellation_token.clone(),
            options,
        )
        .await?;

        {
            let mut torrents = self.torrents.write().await;

            if torrents.contains_key(&info_hash) {
                return Err(Error::TorrentAlreadyExists);
            }

            torrents.insert(info_hash, torrent_handle);
        }

        Ok(info_hash)
    }

    pub async fn pause_torrent(&self, info_hash: InfoHash) {
        todo!()
    }
    pub async fn resume_torrent(&self, info_hash: InfoHash) {
        todo!()
    }
    pub async fn remove_torrent(&self, info_hash: InfoHash) {
        todo!()
    }
    pub async fn remove_torrent_data(&self, info_hash: InfoHash) {
        todo!()
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        debug!("running drop for client, shutting down all tasks via cancellation token");
        // inform the tasks to cancel
        self.cancellation_token.cancel();
        debug!("cancelled cancellation token");
        self.task_tracker.close();
        debug!("closed task tracker");

        debug!("waiting for task tracker");
        // is this correct? is it safe? no idea
        std::thread::scope(|s| {
            s.spawn(|| {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap();
                rt.block_on(self.task_tracker.wait());
                debug!("task tracker done, all tasks are shutdown");
            });
        });
        // block and wait for the tasks to actually shut down
    }
}
