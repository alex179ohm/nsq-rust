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

use std::error::Error;
use std::{fmt, io};

#[derive(Debug)]
pub enum NsqError {
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

impl Error for NsqError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            e => Some(e),
        }
    }
}

impl From<&str> for NsqError {
    fn from(s: &str) -> NsqError {
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
            s => unreachable!(s),
        }
    }
}

#[derive(Debug)]
pub enum ClientError {
    Json(serde_json::Error),
    Io(io::Error),
    Nsq(NsqError),
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Json(e) => write!(f, "{}", e),
            Self::Io(e) => write!(f, "{}", e),
            Self::Nsq(e) => write!(f, "{}", e),
        }
    }
}

impl Error for ClientError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::Json(e) => Some(e),
            Self::Nsq(e) => Some(e),
        }
    }
}

impl From<&str> for ClientError {
    fn from(e: &str) -> Self {
        ClientError::Nsq(NsqError::from(e))
    }
}

impl From<serde_json::Error> for ClientError {
    fn from(e: serde_json::Error) -> Self {
        ClientError::Json(e)
    }
}

impl From<io::Error> for ClientError {
    fn from(e: io::Error) -> Self {
        ClientError::Io(e)
    }
}

impl From<ClientError> for io::Error {
    fn from(e: ClientError) -> Self {
        io::Error::new(io::ErrorKind::Other, format!("{}", e))
    }
}
