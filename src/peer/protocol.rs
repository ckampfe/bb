use crate::torrent::Pieces;
use bytes::{Buf, BytesMut};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use tokio_util::codec::{Decoder, Encoder};

#[derive(Debug, Error)]
pub(super) enum Error {
    #[error("incomplete")]
    Incomplete,
    #[error("connection reset by remote peer")]
    ConnectionReset,
    #[error("unsupported message type")]
    UnsupportedMessageType,
    #[error("io")]
    Io(#[from] std::io::Error),
}

const ONE_MB: usize = 1048576;
const MAX: usize = 8 * ONE_MB;

pub(super) struct PeerProtocol {}

impl PeerProtocol {
    pub(super) fn new() -> Self {
        Self {}
    }

    const CHOKE: u8 = 0;
    const UNCHOKE: u8 = 1;
    const INTERESTED: u8 = 2;
    const NOT_INTERESTED: u8 = 3;
    const HAVE: u8 = 4;
    const BITFIELD: u8 = 5;
    const REQUEST: u8 = 6;
    const PIECE: u8 = 7;
    const CANCEL: u8 = 8;
}

impl Decoder for PeerProtocol {
    type Item = Frame;

    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            // Not enough data to read length marker.
            return Ok(None);
        }

        let mut length_bytes = [0u8; 4];
        length_bytes.copy_from_slice(&src[..4]);
        let length = u32::from_le_bytes(length_bytes) as usize;

        if length > MAX {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Frame of length {} is too large.", length),
            ));
        }

        if src.len() < 4 + length {
            // The full string has not yet arrived.
            //
            // We reserve more space in the buffer. This is not strictly
            // necessary, but is a good idea performance-wise.
            src.reserve(4 + length - src.len());

            // We inform the Framed that we need more bytes to form the next
            // frame.
            return Ok(None);
        }

        let data = src[4..4 + length].to_vec();
        src.advance(4 + length);

        let mut rest = &data[..];

        match rest.get_u8() {
            Self::CHOKE => Ok(Some(Frame::Choke)),
            Self::UNCHOKE => Ok(Some(Frame::Unchoke)),
            Self::INTERESTED => Ok(Some(Frame::Interested)),
            Self::NOT_INTERESTED => Ok(Some(Frame::NotInterested)),
            Self::HAVE => {
                let index = rest.get_u32();
                Ok(Some(Frame::Have { index }))
            }
            Self::BITFIELD => {
                let pieces = Pieces::from_slice(rest.chunk());

                Ok(Some(Frame::Bitfield { pieces }))
            }
            Self::REQUEST => {
                let index = rest.get_u32();
                let begin = rest.get_u32();
                let length = rest.get_u32();
                Ok(Some(Frame::Request {
                    index,
                    begin,
                    length,
                }))
            }
            Self::PIECE => {
                let index = rest.get_u32();
                let begin = rest.get_u32();
                let block = rest.chunk().to_owned();

                // todo this is probably a bug
                Ok(Some(Frame::Piece {
                    index,
                    begin,
                    block,
                }))
            }
            Self::CANCEL => Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "cancel messages are not yet implemented",
            )),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "received unknown message type",
            )),
        }
    }
}

impl Encoder<Frame> for PeerProtocol {
    type Error = std::io::Error;

    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        todo!()
    }
}

pub(super) enum Frame {
    Keepalive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have {
        index: u32,
    },
    Bitfield {
        pieces: Pieces,
    },
    Request {
        index: u32,
        begin: u32,
        length: u32,
    },
    Piece {
        index: u32,
        begin: u32,
        block: Vec<u8>,
    },
    Cancel {
        index: u32,
        begin: u32,
        length: u32,
    },
}
