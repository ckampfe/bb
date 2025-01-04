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
// - [x] get rid of global statics, make everything live within a Client type or something like that,
//       this cleans up the resource usage of this library,
//       and it allows for starting multiple instances of the library in the same process for,
//       e.g., testing
// - [x] figure out how to keep client alive.
//       answer for now: don't drop it.
// - [x] figure out graceful shutdown. https://tokio.rs/tokio/topics/shutdown
// - [ ] figure out if it makes sense to download to something like `filename.jpg.part` vs. just `filename.jpg`

use base64::Engine;
use std::fmt::Debug;
use thiserror::Error;

mod bencode;
mod client;
mod download;
mod metainfo;
mod peer;
pub mod torrent;

pub use client::{Client, Options as ClientOptions};
pub use torrent::Options as TorrentOptions;

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
