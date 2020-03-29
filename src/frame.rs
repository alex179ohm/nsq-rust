// MIT License
//
// Copyright (c) 2019 Alessandro Cresto Miseroglio <alex179ohm@gmail.com>
// Copyright (c) 2019 Tangram Technologies S.R.L. <https://tngrm.io>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

use crate::codec::Decoder;
use crate::error::Error;
use crate::msg::Msg;
use crate::response::Response;
use byteorder::{BigEndian, ByteOrder};
use std::convert::TryFrom;
use std::fmt;
use std::io;

const FRAME_RESPONSE: i32 = 0x1;
const FRAME_ERROR: i32 = 0x2;
const FRAME_MSG: i32 = 0x3;
const FRAME_HEADER_SIZE: usize = 8;

pub enum Frame {
    Response(Response),
    Msg(Msg),
    Error(Error),
}

impl From<Msg> for Frame {
    fn from(msg: Msg) -> Self {
        Frame::Msg(msg)
    }
}

impl From<Response> for Frame {
    fn from(resp: Response) -> Self {
        Frame::Response(resp)
    }
}

impl From<Error> for Frame {
    fn from(error: Error) -> Self {
        Frame::Error(error)
    }
}

impl TryFrom<&[u8]> for Frame {
    type Error = io::Error;
    fn try_from(buf: &[u8]) -> Result<Self, <Self as TryFrom<&[u8]>>::Error> {
        let size = BigEndian::read_i32(&buf[..4]) as usize;

        if size < FRAME_HEADER_SIZE {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unable to parse frame: {:?}", &buf[..]),
            ))
        } else {
            match BigEndian::read_i32(&buf[4..8]) {
                FRAME_MSG => {
                    let msg = Msg::decode(&buf[8..size])?;
                    Ok(Frame::from(msg))
                }
                FRAME_RESPONSE => {
                    let response = Response::decode(&buf[8..size])?;
                    Ok(Frame::from(response))
                }
                FRAME_ERROR => {
                    let err = Error::decode(&buf[8..size])?;
                    Ok(Frame::Error(err))
                }
                s => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unsupported frame type: {}", s),
                )),
            }
        }
    }
}

impl fmt::Debug for Frame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Frame::Response(r) => write!(f, "{:?}", r),
            Frame::Msg(m) => write!(f, "{:?}", m),
            Frame::Error(e) => write!(f, "{:?}", e),
        }
    }
}
