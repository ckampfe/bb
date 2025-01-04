use crate::torrent::{self, TorrentHandle};
use crate::{Error, InfoHash};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::debug;

/// global options
pub struct Options {
    pub max_connections: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            max_connections: 200,
        }
    }
}

// should this be one pointer to an inner struct?
#[derive(Clone)]
pub struct Client {
    torrents: Arc<RwLock<BTreeMap<InfoHash, TorrentHandle>>>,
    global_max_connections: Arc<tokio::sync::Semaphore>,
    /// every subtask spawned for either a torrent or a peer connection
    /// receives this cancellation token,
    /// which is run when the client is dropped.
    task_tracker: TaskTracker,
    cancellation_token: CancellationToken,
}

impl Client {
    pub fn new(options: Options) -> Self {
        Self {
            torrents: Arc::new(RwLock::new(BTreeMap::new())),
            global_max_connections: Arc::new(tokio::sync::Semaphore::new(options.max_connections)),
            task_tracker: TaskTracker::new(),
            cancellation_token: CancellationToken::new(),
        }
    }

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
                let rt = tokio::runtime::Builder::new_multi_thread().build().unwrap();
                rt.block_on(self.task_tracker.wait());
                debug!("task tracker done, all tasks are shutdown");
            });
        });
        // block and wait for the tasks to actually shut down
    }
}
