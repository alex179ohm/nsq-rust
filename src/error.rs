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
use std::convert::TryFrom;
use std::fmt;
use std::io;
use std::str;

pub enum Error {
    Invalid,
    Body,
    Topic,
    Channel,
    Message,
    Pub,
    Mpub,
    Dpub,
    Fin,
    Req,
    Touch,
    Auth,
    Unauthorized,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Invalid => write!(f, "E_INVALID"),
            Self::Body => write!(f, "E_BAD_BODY"),
            Self::Topic => write!(f, "E_BAD_TOPIC"),
            Self::Channel => write!(f, "E_BAD_CHANNEL"),
            Self::Message => write!(f, "E_BAD_MESSAGE"),
            Self::Pub => write!(f, "E_PUB_FAILED"),
            Self::Mpub => write!(f, "E_MPUB_FAILED"),
            Self::Dpub => write!(f, "E_DPUB_FAILED"),
            Self::Fin => write!(f, "E_FIN_FAILED"),
            Self::Req => write!(f, "E_REQ_FAILED"),
            Self::Touch => write!(f, "E_TOUCH_FAILED"),
            Self::Auth => write!(f, "E_AUTH_FAILED"),
            Self::Unauthorized => write!(f, "E_UNAUTHORIZED"),
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self)
    }
}

impl TryFrom<&str> for Error {
    type Error = io::Error;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "E_INVALID" => Ok(Error::Invalid),
            "E_BAD_BODY" => Ok(Error::Body),
            "E_BAD_TOPIC" => Ok(Error::Topic),
            "E_BAD_CHANNEL" => Ok(Error::Channel),
            "E_BAD_MESSAGE" => Ok(Error::Message),
            "E_PUB_FAILED" => Ok(Error::Pub),
            "E_MPUB_FAILED" => Ok(Error::Mpub),
            "E_DPUB_FAILED" => Ok(Error::Dpub),
            "E_FIN_FAILED" => Ok(Error::Fin),
            "E_REQ_FAILED" => Ok(Error::Req),
            "E_TOUCH_FAILED" => Ok(Error::Touch),
            "E_AUTH_FAILED" => Ok(Error::Auth),
            "E_UNAUTHORIZED" => Ok(Error::Unauthorized),
            s => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("error on parsing: {:?}", s),
            )),
        }
    }
}

impl Decoder for Error {
    type Error = io::Error;
    fn decode(buf: &[u8]) -> Result<Self, Self::Error> {
        let s = str::from_utf8(buf)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{}", e)))?;
        Error::try_from(s)
    }
}
