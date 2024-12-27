use crate::torrent::Pieces;
use crate::InfoHash;
use bytes::{Buf, BufMut, BytesMut};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use tokio_util::codec::{Decoder, Encoder};

use super::PeerId;

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
const FRAME_RECEIVE_MAX: usize = 8 * ONE_MB;

pub(super) struct HandshakeProtocol {
    info_hash: InfoHash,
    remote_peer_id: Option<PeerId>,
}

impl HandshakeProtocol {
    pub(super) fn new(info_hash: InfoHash, remote_peer_id: Option<PeerId>) -> Self {
        Self {
            info_hash,
            remote_peer_id,
        }
    }

    const HANDSHAKE_LENGTH: usize = 1 + 19 + 8 + 20 + 20;
}

impl Decoder for HandshakeProtocol {
    type Item = PeerId;

    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < Self::HANDSHAKE_LENGTH {
            // Not enough data
            return Ok(None);
        }

        if src.len() > Self::HANDSHAKE_LENGTH {
            return Err(std::io::Error::new(
                std::io::ErrorKind::FileTooLarge,
                format!("Frame of length {} is too large.", src.len()),
            ));
        }

        let len_byte = src.get_u8();
        if len_byte != 19 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "first byte not 19",
            ));
        }

        if &src[..19] != b"BitTorrent protocol" {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Not 'BitTorrent protocol'",
            ));
        }

        src.advance(19);

        // ignore protocol options
        src.advance(8);

        if &src[..20] != &self.info_hash.0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Bad info hash",
            ));
        }

        src.advance(20);

        let challenge_peer_id = &src[..20];
        let challenge_peer_id: [u8; 20] = challenge_peer_id.try_into().unwrap();
        let challenge_peer_id = PeerId(challenge_peer_id);

        // if there is a peer id, we compare it.
        // if not, the tracker is using compact peers,
        // so we don't yet have a peer id until the peer sends it to us,
        // so we can't compare it with something the tracker never gave us.
        if let Some(remote_peer_id) = self.remote_peer_id {
            if challenge_peer_id != remote_peer_id {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Bad info peer id",
                ));
            }
        }

        Ok(Some(challenge_peer_id))
    }
}

/// A marker type on which to implement `Decoder` and `Encoder`.
/// We then pass this type to `FramedRead` and `FramedWrite`
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
        let length = u32::from_be_bytes(length_bytes) as usize;

        // basically, we don't want to get DDoS'd,
        // so impose *some* limit here.
        if length > FRAME_RECEIVE_MAX {
            return Err(std::io::Error::new(
                std::io::ErrorKind::FileTooLarge,
                format!("Frame of length {} is too large.", length),
            ));
        }

        if length == 0 {
            return Ok(Some(Frame::Keepalive));
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
                let block = rest.to_vec();

                Ok(Some(Frame::Piece {
                    index,
                    begin,
                    block,
                }))
            }
            Self::CANCEL => {
                let index = rest.get_u32();
                let begin = rest.get_u32();
                let length = rest.get_u32();
                Ok(Some(Frame::Cancel {
                    index,
                    begin,
                    length,
                }))
            }
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
        match item {
            Frame::Keepalive => {
                let len: u32 = 0;
                let len_slice = [0, 0, 0, 0];

                dst.reserve(len_slice.len() + len as usize);

                dst.extend_from_slice(&len_slice);
            }
            Frame::Choke => {
                let len: u32 = 1;
                let len_slice = len.to_be_bytes();

                dst.reserve(len_slice.len() + len as usize);

                dst.extend_from_slice(&len_slice);
                dst.put_u8(Self::CHOKE);
            }
            Frame::Unchoke => {
                let len: u32 = 1;
                let len_slice = len.to_be_bytes();

                dst.reserve(len_slice.len() + len as usize);

                dst.extend_from_slice(&len_slice);
                dst.put_u8(Self::UNCHOKE);
            }
            Frame::Interested => {
                let len: u32 = 1;
                let len_slice = len.to_be_bytes();

                dst.reserve(len_slice.len() + len as usize);

                dst.extend_from_slice(&len_slice);
                dst.put_u8(Self::INTERESTED);
            }
            Frame::NotInterested => {
                let len = 1u32;
                let len_slice = len.to_be_bytes();

                dst.reserve(len_slice.len() + len as usize);
                dst.extend_from_slice(&len_slice);
                dst.put_u8(Self::NOT_INTERESTED);
            }
            Frame::Have { index } => {
                let index_slice = index.to_be_bytes();
                let len: u32 = 1 + index_slice.len() as u32;
                let len_slice = len.to_be_bytes();

                dst.reserve(len_slice.len() + len as usize);

                dst.extend_from_slice(&len_slice);
                dst.put_u8(Self::HAVE);
                dst.extend_from_slice(&index_slice);
            }
            Frame::Bitfield { pieces } => {
                let pieces_slice = pieces.as_raw_slice();
                let len: u32 = 1 + pieces_slice.len() as u32;
                let len_slice = len.to_be_bytes();

                dst.reserve(len_slice.len() + len as usize);

                dst.extend_from_slice(&len_slice);
                dst.put_u8(Self::BITFIELD);
                dst.extend_from_slice(pieces_slice);
            }
            Frame::Request {
                index,
                begin,
                length,
            } => {
                let index_slice = index.to_be_bytes();
                let begin_slice = begin.to_be_bytes();
                let length_slice = length.to_be_bytes();

                let len: u32 = 1
                    + index_slice.len() as u32
                    + begin_slice.len() as u32
                    + length_slice.len() as u32;

                let len_slice = len.to_be_bytes();

                dst.reserve(len_slice.len() + len as usize);

                dst.extend_from_slice(&len_slice);
                dst.put_u8(Self::REQUEST);
                dst.extend_from_slice(&index_slice);
                dst.extend_from_slice(&begin_slice);
                dst.extend_from_slice(&length_slice);
            }
            Frame::Piece {
                index,
                begin,
                block,
            } => {
                let index_slice = index.to_be_bytes();
                let begin_slice = begin.to_be_bytes();

                let len: u32 = 1
                    + index_slice.len() as u32
                    + begin_slice.len() as u32
                    + u32::try_from(block.len())
                        .expect("block must have a length that can fit into a u32");
                let len_slice = len.to_be_bytes();

                dst.reserve(len_slice.len() + len as usize);

                dst.extend_from_slice(&len_slice);
                dst.put_u8(Self::PIECE);
                dst.extend_from_slice(&index_slice);
                dst.extend_from_slice(&begin_slice);
                dst.extend_from_slice(&block);
            }
            Frame::Cancel {
                index,
                begin,
                length,
            } => {
                let index_slice = index.to_be_bytes();
                let begin_slice = begin.to_be_bytes();
                let length_slice = length.to_be_bytes();

                let len: u32 = 1
                    + index_slice.len() as u32
                    + begin_slice.len() as u32
                    + length_slice.len() as u32;
                let len_slice = len.to_be_bytes();

                dst.reserve(len_slice.len() + len as usize);

                dst.extend_from_slice(&len_slice);
                dst.put_u8(Self::CANCEL);
                dst.extend_from_slice(&index_slice);
                dst.extend_from_slice(&begin_slice);
                dst.extend_from_slice(&length_slice);
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
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

#[cfg(test)]
mod tests {
    use futures::SinkExt;
    use rand::{thread_rng, Rng};
    use tokio_stream::StreamExt;
    use tokio_util::codec::{FramedRead, FramedWrite};

    use crate::torrent::Pieces;

    use super::{Frame, PeerProtocol};

    #[tokio::test]
    async fn roundtrips_keepalive() {
        let buf = vec![];
        let mut writer = FramedWrite::new(buf, PeerProtocol::new());
        writer.send(Frame::Keepalive).await.unwrap();
        let out = writer.get_ref();

        let mut reader = FramedRead::new(&out[..], PeerProtocol::new());

        assert_eq!(reader.next().await.unwrap().unwrap(), Frame::Keepalive);
    }

    #[tokio::test]
    async fn roundtrips_choke() {
        let buf = vec![];
        let mut writer = FramedWrite::new(buf, PeerProtocol::new());
        writer.send(Frame::Choke).await.unwrap();
        let out = writer.get_ref();

        let mut reader = FramedRead::new(&out[..], PeerProtocol::new());

        assert_eq!(reader.next().await.unwrap().unwrap(), Frame::Choke);
    }

    #[tokio::test]
    async fn roundtrips_unchoke() {
        let buf = vec![];
        let mut writer = FramedWrite::new(buf, PeerProtocol::new());
        writer.send(Frame::Unchoke).await.unwrap();
        let out = writer.get_ref();

        let mut reader = FramedRead::new(&out[..], PeerProtocol::new());

        assert_eq!(reader.next().await.unwrap().unwrap(), Frame::Unchoke);
    }

    #[tokio::test]
    async fn roundtrips_interested() {
        let buf = vec![];
        let mut writer = FramedWrite::new(buf, PeerProtocol::new());
        writer.send(Frame::Interested).await.unwrap();
        let out = writer.get_ref();

        let mut reader = FramedRead::new(&out[..], PeerProtocol::new());

        assert_eq!(reader.next().await.unwrap().unwrap(), Frame::Interested);
    }

    #[tokio::test]
    async fn roundtrips_not_interested() {
        let buf = vec![];
        let mut writer = FramedWrite::new(buf, PeerProtocol::new());
        writer.send(Frame::NotInterested).await.unwrap();
        let out = writer.get_ref();

        let mut reader = FramedRead::new(&out[..], PeerProtocol::new());

        assert_eq!(reader.next().await.unwrap().unwrap(), Frame::NotInterested);
    }

    #[tokio::test]
    async fn roundtrips_have() {
        let buf = vec![];
        let mut writer = FramedWrite::new(buf, PeerProtocol::new());

        let index = rand::random();

        writer.send(Frame::Have { index }).await.unwrap();
        let out = writer.get_ref();

        let mut reader = FramedRead::new(&out[..], PeerProtocol::new());

        assert_eq!(reader.next().await.unwrap().unwrap(), Frame::Have { index });
    }

    #[tokio::test]
    async fn roundtrips_bitfield() {
        let buf = vec![];
        let mut writer = FramedWrite::new(buf, PeerProtocol::new());

        let mut rng = thread_rng();

        let raw_pieces_length = rng.gen_range(0..500);
        let mut pieces_raw = vec![0; raw_pieces_length];
        rng.fill(&mut pieces_raw[..]);

        let pieces = Pieces::from_slice(&pieces_raw);

        writer
            .send(Frame::Bitfield {
                pieces: pieces.clone(),
            })
            .await
            .unwrap();
        let out = writer.get_ref();

        let mut reader = FramedRead::new(&out[..], PeerProtocol::new());

        assert_eq!(
            reader.next().await.unwrap().unwrap(),
            Frame::Bitfield { pieces }
        );
    }

    #[tokio::test]
    async fn roundtrips_request() {
        let buf = vec![];
        let mut writer = FramedWrite::new(buf, PeerProtocol::new());

        let index = rand::random();
        let begin = rand::random();
        let length = rand::random();

        writer
            .send(Frame::Request {
                index,
                begin,
                length,
            })
            .await
            .unwrap();
        let out = writer.get_ref();

        let mut reader = FramedRead::new(&out[..], PeerProtocol::new());

        assert_eq!(
            reader.next().await.unwrap().unwrap(),
            Frame::Request {
                index,
                begin,
                length
            }
        );
    }

    #[tokio::test]
    async fn roundtrips_piece() {
        let buf = vec![];
        let mut writer = FramedWrite::new(buf, PeerProtocol::new());

        let mut rng = thread_rng();

        let index = rand::random();
        let begin = rand::random();
        let block_length = rng.gen_range(0..500);
        let mut block = vec![0; block_length];
        rng.fill(&mut block[..]);

        writer
            .send(Frame::Piece {
                index,
                begin,
                block: block.clone(),
            })
            .await
            .unwrap();
        let out = writer.get_ref();

        let mut reader = FramedRead::new(&out[..], PeerProtocol::new());

        assert_eq!(
            reader.next().await.unwrap().unwrap(),
            Frame::Piece {
                index,
                begin,
                block
            }
        );
    }

    #[tokio::test]
    async fn roundtrips_cancel() {
        let buf = vec![];
        let mut writer = FramedWrite::new(buf, PeerProtocol::new());

        let index = rand::random();
        let begin = rand::random();
        let length = rand::random();

        writer
            .send(Frame::Cancel {
                index,
                begin,
                length,
            })
            .await
            .unwrap();
        let out = writer.get_ref();

        let mut reader = FramedRead::new(&out[..], PeerProtocol::new());

        assert_eq!(
            reader.next().await.unwrap().unwrap(),
            Frame::Cancel {
                index,
                begin,
                length
            }
        );
    }
}
