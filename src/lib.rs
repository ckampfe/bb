use bencode::Bencode;
use std::{collections::BTreeMap, path::Path};
use thiserror::Error;
use tokio::sync::Semaphore;
use torrent::{InfoHash, Pieces, TorrentHandle, TorrentState};

mod bencode;
mod metainfo;
mod peer;
pub mod torrent;

#[derive(Debug, Error)]
pub enum Error {
    #[error("torrent error")]
    Torrent(#[from] torrent::Error),
}

static TORRENTS: tokio::sync::RwLock<BTreeMap<InfoHash, TorrentHandle>> =
    tokio::sync::RwLock::const_new(BTreeMap::new());

static MAX_CONNECTIONS: tokio::sync::Semaphore = Semaphore::const_new(100);

pub async fn new_torrent<P: AsRef<Path>>(
    dot_torrent_path: P,
    data_path: P,
    options: torrent::Options,
) -> Result<InfoHash, Error> {
    let (info_hash, torrent) = TorrentState::new(dot_torrent_path, data_path, options).await?;
    {
        let mut torrents = TORRENTS.write().await;
        torrents.insert(info_hash, torrent);
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
        let announce_response = torrent.announce().await?;
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
        let pieces = torrent.get_state().await?;
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
