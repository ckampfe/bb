use base64::Engine;
use bencode::Bencode;
use std::fmt::Debug;
use std::{collections::BTreeMap, path::Path};
use thiserror::Error;
use tokio::sync::Semaphore;
use torrent::{Pieces, TorrentHandle};

mod bencode;
mod metainfo;
mod peer;
pub mod torrent;

/// uniquely identifies a torrent
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct InfoHash(pub [u8; 20]);

impl Debug for InfoHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let base64 = base64::prelude::BASE64_STANDARD.encode(self.0);
        write!(f, "{}", base64)
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("torrent error")]
    Torrent(#[from] torrent::Error),
}

// is this a good idea? bad idea?
// should we have an object that holds this data instead of it being a static? idk
static TORRENTS: tokio::sync::RwLock<BTreeMap<InfoHash, TorrentHandle>> =
    tokio::sync::RwLock::const_new(BTreeMap::new());

// todo do this by config
static GLOBAL_MAX_CONNECTIONS: tokio::sync::Semaphore = Semaphore::const_new(200);

pub async fn new_torrent<P: AsRef<Path>>(
    dot_torrent_path: P,
    data_path: P,
    options: torrent::Options,
) -> Result<InfoHash, Error> {
    let (info_hash, torrent_handle) = torrent::new(dot_torrent_path, data_path, options).await?;
    {
        let mut torrents = TORRENTS.write().await;
        torrents.insert(info_hash, torrent_handle);
    }

    Ok(info_hash)
}

pub async fn verify_local_data(info_hash: InfoHash) -> Result<(), Error> {
    let torrents = TORRENTS.read().await;
    if let Some(torrent) = torrents.get(&info_hash) {
        torrent.verify_local_data().await?;
        Ok(())
    } else {
        panic!()
    }
}

pub async fn force_announce(info_hash: InfoHash) -> Result<Bencode, Error> {
    let torrents = TORRENTS.read().await;
    if let Some(torrent) = torrents.get(&info_hash) {
        let announce_response = torrent.force_announce().await?;
        Ok(announce_response)
    } else {
        panic!()
    }
}

pub async fn get_pieces(info_hash: InfoHash) -> Result<Pieces, Error> {
    let torrents = TORRENTS.read().await;
    if let Some(torrent) = torrents.get(&info_hash) {
        let pieces = torrent.get_pieces().await?;
        Ok(pieces)
    } else {
        panic!()
    }
}

pub async fn get_state(info_hash: InfoHash) -> Result<String, Error> {
    let torrents = TORRENTS.read().await;
    if let Some(torrent) = torrents.get(&info_hash) {
        let pieces = torrent.get_state_debug_string().await?;
        Ok(pieces)
    } else {
        panic!()
    }
}

/// timeout seconds future
#[macro_export]
macro_rules! timeout {
    ($f:expr, $secs:expr) => {
        tokio::time::timeout(std::time::Duration::from_secs($secs), $f)
    };
}

pub(crate) fn hash(bytes: &[u8]) -> [u8; 20] {
    let mut hasher = sha1_smol::Sha1::new();
    hasher.update(bytes);
    hasher.digest().bytes()
}

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn it_works() {
//         let result = add(2, 2);
//         assert_eq!(result, 4);
//     }
// }
