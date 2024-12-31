use crate::bencode::{self, Bencode};
use crate::InfoHash;
use std::ops::Rem;

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
        self.length().div_ceil(self.piece_length() as u64)
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

    pub(crate) fn piece_length(&self) -> u32 {
        self.input
            .get("info")
            .and_then(|b| b.get("piece length"))
            .unwrap()
            .as_u32()
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

    pub(crate) fn piece_hash(&self, piece_index: u32) -> Option<&[u8; 20]> {
        self.piece_hashes_iter().nth(piece_index as usize)
    }

    pub(crate) fn blocks_for_piece(&self, piece_index: u32, block_length: u32) -> Vec<Block> {
        let actual_piece_length = self.actual_piece_length(piece_index);

        let number_of_full_blocks: u32 = actual_piece_length / block_length;

        let nominal_piece_length = self.piece_length();

        let mut blocks = (0..number_of_full_blocks)
            .map(|block_number| Block {
                begin: block_number * block_length,
                length: block_length,
            })
            .collect::<Vec<_>>();

        if actual_piece_length < nominal_piece_length {
            let current_last = blocks.last().unwrap();

            let last_block_length =
                actual_piece_length - (current_last.begin + current_last.length);

            blocks.push(Block {
                begin: current_last.begin + current_last.length,
                length: last_block_length,
            });

            blocks
        } else {
            blocks
        }
    }

    pub(crate) fn actual_piece_length(&self, piece_index: u32) -> u32 {
        if self.is_last_piece(piece_index) {
            self.last_piece_length()
        } else {
            self.piece_length()
        }
    }

    fn last_piece_length(&self) -> u32 {
        let actual_length = self.length();

        if actual_length.rem(self.piece_length() as u64) == 0 {
            self.piece_length()
        } else {
            let length_as_if_exact_multiple_of_piece_length =
                self.number_of_pieces() * self.piece_length() as u64;

            let last_piece_length = actual_length
                - (length_as_if_exact_multiple_of_piece_length - self.piece_length() as u64);

            u32::try_from(last_piece_length).unwrap()
        }
    }

    fn is_last_piece(&self, piece_index: u32) -> bool {
        piece_index as u64 == self.number_of_pieces() - 1
    }

    /// the offset of the piece within the download
    pub(crate) fn piece_offset(&self, piece_index: u32) -> u64 {
        piece_index as u64 * self.piece_length() as u64
    }
}

pub(crate) struct Block {
    pub(crate) begin: u32,
    pub(crate) length: u32,
}
