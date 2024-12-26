use nom::branch::alt;
use nom::bytes::complete::{tag, take};
use nom::character::complete::digit1;
use nom::combinator::opt;
use nom::error::{ErrorKind, ParseError};
use nom::multi::many0;
use nom::IResult;
use std::collections::{BTreeMap, VecDeque};
use std::fmt::Debug;
use std::io::Write;
use std::num::ParseIntError;
use std::str::Utf8Error;
use thiserror::Error;

#[derive(Clone, PartialEq, PartialOrd, Eq, Ord)]
pub enum Bencode {
    Dict(BTreeMap<String, Bencode>),
    List(Vec<Bencode>),
    Integer(i64),
    ByteString(Vec<u8>),
}

impl Debug for Bencode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Bencode::Dict(ref btree_map) => f
                .debug_map()
                .entries(btree_map.iter().map(|(k, v)| (k, v)))
                .finish(),
            Bencode::List(vec) => f.debug_list().entries(vec.iter()).finish(),
            Bencode::Integer(i) => write!(f, "Integer({})", i),

            Bencode::ByteString(v) => match std::str::from_utf8(v) {
                Ok(s) => write!(f, "String({})", s),
                Err(e) => write!(f, "ByteString({:?})", v),
            },
        }
    }
}

impl Bencode {
    pub(crate) fn get(&self, key: &str) -> Option<&Bencode> {
        match self {
            Bencode::Dict(btree_map) => btree_map.get(key),
            _ => unimplemented!(), // Bencode::List(vec) => todo!(),
                                   // Bencode::Integer(_) => todo!(),
                                   // Bencode::ByteString(_) => todo!(),
        }
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        if let Bencode::ByteString(b) = self {
            b
        } else {
            panic!("not bytes")
        }
    }

    pub(crate) fn as_i64(&self) -> i64 {
        if let Bencode::Integer(i) = self {
            *i
        } else {
            panic!("not integer")
        }
    }

    pub(crate) fn as_u16(&self) -> u16 {
        if let Bencode::Integer(i) = self {
            u16::try_from(*i).unwrap()
        } else {
            panic!("could not get as u16")
        }
    }

    pub(crate) fn as_u64(&self) -> u64 {
        if let Bencode::Integer(i) = self {
            u64::try_from(*i).unwrap()
        } else {
            panic!("not integer")
        }
    }

    pub(crate) fn as_str(&self) -> &str {
        if let Bencode::ByteString(s) = self {
            std::str::from_utf8(s).unwrap()
        } else {
            panic!("not str")
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum CustomError<I> {
    Nom(I, nom::error::ErrorKind),
    NotUtf8(Utf8Error),
    NotDigits(ParseIntError),
}

impl<I> nom::error::ParseError<I> for CustomError<I> {
    fn from_error_kind(input: I, kind: nom::error::ErrorKind) -> Self {
        CustomError::Nom(input, kind)
    }

    fn append(_: I, _: ErrorKind, other: Self) -> Self {
        #[derive(Debug, PartialEq)]
        pub enum CustomError<I> {
            MyError,
            Nom(I, ErrorKind),
        }

        impl<I> ParseError<I> for CustomError<I> {
            fn from_error_kind(input: I, kind: ErrorKind) -> Self {
                CustomError::Nom(input, kind)
            }

            fn append(_: I, _: ErrorKind, other: Self) -> Self {
                other
            }
        }

        other
    }
}

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error("could not decode from bencoded data")]
    DecodeError(String),
    #[error("could not encode to bencode data")]
    EncodeError(#[from] std::io::Error),
}

pub(crate) fn decode(input: &[u8]) -> Result<Bencode, Error> {
    let (_input, out) = any(input).unwrap();
    Ok(out)
}

pub(crate) fn encode<W: Write>(bencode: &Bencode, out: &mut W) -> Result<(), Error> {
    let mut to_encode = VecDeque::new();
    to_encode.push_back(ToEncode::Bencode(bencode));

    while let Some(el) = to_encode.pop_front() {
        match el {
            ToEncode::Bencode(bencode) => match bencode {
                Bencode::Integer(i) => {
                    out.write_all(b"i")?;
                    let mut buffer = itoa::Buffer::new();
                    let i_out = buffer.format(*i).as_bytes();
                    out.write_all(i_out)?;
                    out.write_all(b"e")?;
                }
                Bencode::ByteString(s) => {
                    let len = s.len();
                    let mut buffer = itoa::Buffer::new();
                    let len_out = buffer.format(len).as_bytes();
                    out.write_all(len_out)?;
                    out.write_all(b":")?;
                    out.write_all(&s)?;
                }
                Bencode::Dict(btree_map) => {
                    to_encode.push_back(ToEncode::D);

                    for (k, v) in btree_map {
                        to_encode.push_back(ToEncode::Key(k));
                        to_encode.push_back(ToEncode::Bencode(v));
                    }

                    to_encode.push_back(ToEncode::E);
                }
                Bencode::List(vec) => {
                    to_encode.push_back(ToEncode::L);

                    for el in vec {
                        to_encode.push_back(ToEncode::Bencode(el));
                    }

                    to_encode.push_back(ToEncode::E);
                }
            },
            ToEncode::Key(s) => {
                let len = s.len();
                let mut buffer = itoa::Buffer::new();
                let len_out = buffer.format(len).as_bytes();
                out.write_all(len_out)?;
                out.write_all(b":")?;
                out.write_all(s.as_bytes())?;
            }
            ToEncode::D => {
                out.write_all(b"d")?;
            }
            ToEncode::E => {
                out.write_all(b"e")?;
            }
            ToEncode::L => {
                out.write_all(b"l")?;
            }
        }
    }

    Ok(())
}

enum ToEncode<'input> {
    Bencode(&'input Bencode),
    Key(&'input str),
    D,
    E,
    L,
}

fn any(input: &[u8]) -> IResult<&[u8], Bencode, CustomError<&[u8]>> {
    alt((dict, list, string, integer))(input)
}

fn dict(input: &[u8]) -> IResult<&[u8], Bencode, CustomError<&[u8]>> {
    let (input, _) = tag(b"d")(input)?;
    let (input, map) = kvs(input)?;
    let (input, _) = tag(b"e")(input)?;

    Ok((input, Bencode::Dict(map)))
}

fn kvs(input: &[u8]) -> IResult<&[u8], BTreeMap<String, Bencode>, CustomError<&[u8]>> {
    let (input, pairs) = many0(kv)(input)?;
    let map = BTreeMap::from_iter(pairs);
    Ok((input, map))
}

fn kv(input: &[u8]) -> IResult<&[u8], (String, Bencode), CustomError<&[u8]>> {
    let (input, k) = dict_key(input)?;
    let (input, v) = any(input)?;
    Ok((input, (k.to_owned(), v)))
}

fn list(input: &[u8]) -> IResult<&[u8], Bencode, CustomError<&[u8]>> {
    let (input, _) = tag(b"l")(input)?;
    let (input, vec) = many0(any)(input)?;
    let (input, _) = tag(b"e")(input)?;

    Ok((input, Bencode::List(vec)))
}

fn integer(input: &[u8]) -> IResult<&[u8], Bencode, CustomError<&[u8]>> {
    let (input, _) = tag(b"i")(input)?;
    let (input, is_negative) = opt(tag(b"-"))(input)?;
    let (input, digits) = digit1(input)?;
    let (input, _) = tag(b"e")(input)?;

    let as_str = std::str::from_utf8(digits).unwrap();
    let mut i = as_str.parse::<i64>().unwrap();
    if is_negative.is_some() {
        i *= -1
    }

    Ok((input, Bencode::Integer(i)))
}

fn dict_key(input: &[u8]) -> IResult<&[u8], &str, CustomError<&[u8]>> {
    let (input, digits) = digit1(input)?;
    let as_str =
        std::str::from_utf8(digits).map_err(|e| nom::Err::Error(CustomError::NotUtf8(e)))?;
    let length = as_str
        .parse::<u64>()
        .map_err(|e| nom::Err::Error(CustomError::NotDigits(e)))?;
    let (input, _) = tag(b":")(input)?;
    let (input, s) = take(length)(input)?;
    Ok((
        input,
        std::str::from_utf8(s).map_err(|e| nom::Err::Error(CustomError::NotUtf8(e)))?,
    ))
}

fn string(input: &[u8]) -> IResult<&[u8], Bencode, CustomError<&[u8]>> {
    let (input, digits) = digit1(input)?;
    let as_str = std::str::from_utf8(digits).unwrap();
    let length = as_str.parse::<u64>().unwrap();
    let (input, _) = tag(b":")(input)?;
    let (input, s) = take(length)(input)?;
    Ok((input, Bencode::ByteString(s.to_vec())))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let input = std::fs::read("a8dmfmt66t211.png.torrent").unwrap();
        let _decoded = decode(&input).unwrap();
    }
}
