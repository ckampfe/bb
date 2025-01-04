// TODO
// - [ ] figure out peer/torrent linkage for error handling.
//       there is a lot of uncertainty here. we need to be able to
//       propagate this failure information so tasks shut down properly.
//       could probably use https://docs.rs/tokio/latest/tokio/sync/index.html#watch-channel
// - [ ] accept incoming connections
// - [ ] have peer tasks send stats...somewhere. we want to be able to use them in the choking algorithm
// - [ ] figure out what actually should be public.
//       current problem is that we have errors that should be opaque but are not,
//       and so they're leaking internal implementation information.
//       figure out how to use thiserror to roll up multiple internal errors into a
//       single opaque error.
// - [ ] get rid of global statics, make everything live within a Client type or something like that,
//       this cleans up the resource usage of this library,
//       and it allows for starting multiple instances of the library in the same process for,
//       e.g., testing

use base64::Engine;
use bencode::Bencode;
use metainfo::MetaInfo;
use std::fmt::Debug;
use std::{collections::BTreeMap, path::Path};
use thiserror::Error;
use tokio::sync::{RwLock, Semaphore};
use torrent::{Pieces, TorrentHandle};

mod bencode;
mod download;
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
    #[error("unable to get pieces for this torrent")]
    NoPieces,
    #[error("can't add torrent because this torrent already exists")]
    TorrentAlreadyExists,
}

// is this a good idea? bad idea?
// should we have an object that holds this data instead of it being a static? idk
static TORRENTS: RwLock<BTreeMap<InfoHash, TorrentHandle>> = RwLock::const_new(BTreeMap::new());
// this is immutable.
// once a metainfo is inserted, it is never modified, unless it is deleted
// due to a torrent being removed
static METAINFOS: RwLock<BTreeMap<InfoHash, MetaInfo>> = RwLock::const_new(BTreeMap::new());

static PIECES: RwLock<BTreeMap<InfoHash, Pieces>> = RwLock::const_new(BTreeMap::new());

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

        if torrents.contains_key(&info_hash) {
            return Err(Error::TorrentAlreadyExists);
        }

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
    let pieces = PIECES.read().await;
    let my_pieces = pieces.get(&info_hash).ok_or(Error::NoPieces)?;
    Ok(my_pieces.to_owned())
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
