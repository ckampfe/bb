use crate::bencode::{self, Bencode};
use crate::torrent::InfoHash;

#[derive(Debug)]
pub(crate) struct MetaInfo {
    pub(crate) input: Bencode,
}

impl MetaInfo {
    pub(crate) fn new(input: Bencode) -> Self {
        Self { input }
    }

    pub(crate) fn info_hash(&self) -> Result<InfoHash, bencode::Error> {
        let info = self.input.get("info").unwrap();
        let mut out = vec![];
        bencode::encode(info, &mut out)?;
        Ok(InfoHash(crate::hash(&out)))
    }

    pub(crate) fn number_of_pieces(&self) -> u64 {
        self.length().div_ceil(self.piece_length())
    }

    pub(crate) fn announce(&self) -> &str {
        self.input.get("announce").unwrap().as_str()
    }

    pub(crate) fn length(&self) -> u64 {
        self.input
            .get("info")
            .and_then(|b| b.get("length"))
            .unwrap()
            .as_u64()
    }

    pub(crate) fn name(&self) -> &str {
        self.input
            .get("info")
            .and_then(|b| b.get("name"))
            .unwrap()
            .as_str()
    }
    pub(crate) fn piece_length(&self) -> u64 {
        self.input
            .get("info")
            .and_then(|b| b.get("piece length"))
            .unwrap()
            .as_u64()
    }

    pub(crate) fn piece_hashes_raw(&self) -> &[u8] {
        self.input
            .get("info")
            .and_then(|b| b.get("pieces"))
            .unwrap()
            .as_bytes()
    }

    pub(crate) fn piece_hashes_iter(&self) -> impl Iterator<Item = &[u8; 20]> {
        self.piece_hashes_raw()
            .chunks_exact(20)
            .map(|c| c.try_into().unwrap())
    }
}
