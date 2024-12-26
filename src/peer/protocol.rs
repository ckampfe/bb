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

// pub struct Connection {
//     stream: BufWriter<TcpStream>,
//     buffer: BytesMut,
// }

// impl Connection {
//     const CHOKE: u8 = 0;
//     const UNCHOKE: u8 = 1;
//     const INTERESTED: u8 = 2;
//     const NOT_INTERESTED: u8 = 3;
//     const HAVE: u8 = 4;
//     const BITFIELD: u8 = 5;
//     const REQUEST: u8 = 6;
//     const PIECE: u8 = 7;
//     const CANCEL: u8 = 8;

//     pub(super) fn new(stream: TcpStream) -> Connection {
//         Connection {
//             stream: BufWriter::new(stream),
//             // Allocate the buffer with 4kb of capacity.
//             buffer: BytesMut::with_capacity(4096),
//         }
//     }

//     pub(super) async fn read_frame(&mut self) -> Result<Option<Frame>, Error> {
//         loop {
//             // Attempt to parse a frame from the buffered data. If
//             // enough data has been buffered, the frame is
//             // returned.
//             if let Some(frame) = self.parse_frame()? {
//                 return Ok(Some(frame));
//             }

//             // There is not enough buffered data to read a frame.
//             // Attempt to read more data from the socket.
//             //
//             // On success, the number of bytes is returned. `0`
//             // indicates "end of stream".
//             if 0 == self.stream.read_buf(&mut self.buffer).await? {
//                 // The remote closed the connection. For this to be
//                 // a clean shutdown, there should be no data in the
//                 // read buffer. If there is, this means that the
//                 // peer closed the socket while sending a frame.
//                 if self.buffer.is_empty() {
//                     return Ok(None);
//                 } else {
//                     return Err(Error::ConnectionReset);
//                 }
//             }
//         }
//     }

//     pub(super) async fn write_frame(&mut self, frame: &Frame) -> std::io::Result<()> {
//         match frame {
//             Frame::Keepalive => self.stream.write_u32(0).await,
//             Frame::Choke => {
//                 self.stream.write_u32(1).await?;
//                 self.stream.write_u8(Self::CHOKE).await
//             }
//             Frame::Unchoke => {
//                 self.stream.write_u32(1).await?;
//                 self.stream.write_u8(Self::UNCHOKE).await
//             }
//             Frame::Interested => {
//                 self.stream.write_u32(1).await?;
//                 self.stream.write_u8(Self::INTERESTED).await
//             }
//             Frame::NotInterested => {
//                 self.stream.write_u32(1).await?;
//                 self.stream.write_u8(Self::NOT_INTERESTED).await
//             }
//             Frame::Have { index } => {
//                 self.stream.write_u32(5).await?;
//                 self.stream.write_u8(Self::HAVE).await?;
//                 self.stream.write_u32(*index).await
//             }
//             Frame::Bitfield { pieces } => todo!(),
//             Frame::Request {
//                 index,
//                 begin,
//                 length,
//             } => {
//                 self.stream.write_u32(13).await?;
//                 self.stream.write_u8(Self::REQUEST).await?;
//                 self.stream.write_u32(*index).await?;
//                 self.stream.write_u32(*begin).await?;
//                 self.stream.write_u32(*length).await
//             }
//             Frame::Piece {
//                 index,
//                 begin,
//                 block,
//             } => {
//                 self.stream.write_u32(9 + block.len() as u32).await?;
//                 self.stream.write_u8(Self::PIECE).await?;
//                 self.stream.write_u32(*index).await?;
//                 self.stream.write_u32(*begin).await?;
//                 self.stream.write_all(block).await
//             }
//             Frame::Cancel {
//                 index,
//                 begin,
//                 length,
//             } => {
//                 self.stream.write_u32(13).await?;
//                 self.stream.write_u8(Self::CANCEL).await?;
//                 self.stream.write_u32(*index).await?;
//                 self.stream.write_u32(*begin).await?;
//                 self.stream.write_u32(*length).await
//             }
//         }
//     }

//     fn parse_frame(&mut self) -> Result<Option<Frame>, Error> {
//         if !self.buffer.remaining() < 4 {
//             return Err(Error::Incomplete);
//         }

//         let length_bytes: [u8; 4] = self.buffer[..4].try_into().unwrap();
//         // let mut lentake = self.buffer.take(4);

//         // let mut length_bytes = [0u8; 4];

//         // (&mut length_bytes[..]).put(&mut lentake);
//         let length: usize = u32::from_be_bytes(length_bytes).try_into().unwrap();

//         self.buffer.advance(4);

//         if length == 0 {
//             return Ok(Some(Frame::Keepalive));
//         }

//         if self.buffer.remaining() < length {
//             return Err(Error::Incomplete);
//         }

//         // let mut rest = self.buffer.take(length);
//         let mut rest = &self.buffer[..length];

//         match rest.get_u8() {
//             Self::CHOKE => Ok(Some(Frame::Choke)),
//             Self::UNCHOKE => Ok(Some(Frame::Unchoke)),
//             Self::INTERESTED => Ok(Some(Frame::Interested)),
//             Self::NOT_INTERESTED => Ok(Some(Frame::NotInterested)),
//             Self::HAVE => {
//                 let index = rest.get_u32();
//                 Ok(Some(Frame::Have { index }))
//             }
//             Self::BITFIELD => {
//                 let pieces = Pieces::from_slice(rest.chunk());

//                 Ok(Some(Frame::Bitfield { pieces }))
//             }
//             Self::REQUEST => {
//                 let index = rest.get_u32();
//                 let begin = rest.get_u32();
//                 let length = rest.get_u32();
//                 Ok(Some(Frame::Request {
//                     index,
//                     begin,
//                     length,
//                 }))
//             }
//             Self::PIECE => {
//                 let index = rest.get_u32();
//                 let begin = rest.get_u32();
//                 let block = rest.chunk().to_owned();

//                 // todo this is probably a bug
//                 Ok(Some(Frame::Piece {
//                     index,
//                     begin,
//                     block,
//                 }))
//             }
//             Self::CANCEL => Err(Error::UnsupportedMessageType),
//             _ => Err(Error::UnsupportedMessageType),
//         }
//     }

//     pub(super) async fn flush(&mut self) -> Result<(), std::io::Error> {
//         self.stream.flush().await
//     }
// }

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
