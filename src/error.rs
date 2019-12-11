// MIT License
//
// Copyright (c) 2019-2021 Alessandro Cresto Miseroglio <alex179ohm@gmail.com>
// Copyright (c) 2019-2021 Tangram Technologies S.R.L. <https://tngrm.io>
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

use serde_json;
use std::error::Error;
use std::{fmt, io};

#[derive(Debug)]
pub enum NsqError {
    Io(io::Error),
    Json(serde_json::Error),
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

impl fmt::Display for NsqError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use NsqError::*;
        match self {
            Io(_) => write!(f, "network failed"),
            Json(e) => write!(f, "json deserialize: {}", e),
            Invalid => write!(f, "E_INVALID"),
            Body => write!(f, "E_BAD_BODY"),
            Topic => write!(f, "E_BAD_TOPIC"),
            Channel => write!(f, "E_BAD_CHANNEL"),
            Message => write!(f, "E_BAD_MESSAGE"),
            Pub => write!(f, "E_PUB_FAILED"),
            Mpub => write!(f, "E_MPUB_FAILED"),
            Dpub => write!(f, "E_DPUB_FAILED"),
            Fin => write!(f, "E_FIN_FAILED"),
            Req => write!(f, "E_REQ_FAILED"),
            Touch => write!(f, "E_TOUCH_FAILED"),
            Auth => write!(f, "E_AUTH_FAILED"),
            Unauthorized => write!(f, "E_UNAUTHORIZED"),
        }
    }
}

impl Error for NsqError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        use NsqError::*;
        match self {
            Io(e) => Some(e),
            Json(e) => Some(e),
            _ => None,
        }
    }
}

impl From<&'_ str> for NsqError {
    fn from(s: &'_ str) -> NsqError {
        match s {
            "E_INVALID" => NsqError::Invalid,
            "E_BAD_BODY" => NsqError::Body,
            "E_BAD_TOPIC" => NsqError::Topic,
            "E_BAD_CHANNEL" => NsqError::Channel,
            "E_BAD_MESSAGE" => NsqError::Message,
            "E_PUB_FAILED" => NsqError::Pub,
            "E_MPUB_FAILED" => NsqError::Mpub,
            "E_DPUB_FAILED" => NsqError::Dpub,
            "E_FIN_FAILED" => NsqError::Fin,
            "E_REQ_FAILED" => NsqError::Req,
            "E_TOUCH_FAILED" => NsqError::Touch,
            "E_AUTH_FAILED" => NsqError::Auth,
            "E_UNAUTHORIZED" => NsqError::Unauthorized,
            _ => unreachable!("invalid error str"),
        }
    }
}

impl From<serde_json::Error> for NsqError {
    fn from(e: serde_json::Error) -> NsqError {
        NsqError::Json(e)
    }
}

impl From<io::Error> for NsqError {
    fn from(e: io::Error) -> Self {
        NsqError::Io(e)
    }
}

impl From<NsqError> for io::Error {
    fn from(e: NsqError) -> Self {
        match e {
            NsqError::Io(e) => e,
            NsqError::Json(e) => io::Error::new(io::ErrorKind::Other, e.description()),
            e => io::Error::new(io::ErrorKind::Other, e.description()),
        }
    }
}
