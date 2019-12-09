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

use std::fmt;
use crate::msg::Msg;

pub enum Response {
    HeartBeat,
    Ok,
    Msg(Msg),
    Json(String),
}

impl Into<Response> for &'_ str {
    fn into(self) -> Response {
        match self {
            "OK" => Response::Ok,
            "CLOSE_WAIT" => Response::Ok,
            "__heartbeat__" => Response::HeartBeat,
            s => Response::Json(String::from(s)),
        }
    }
}

impl Into<Response> for (i64, u16, String, Vec<u8>) {
    fn into(self) -> Response {
        Response::Msg(self.into())
    }
}

impl Into<Response> for Msg {
    fn into(self) -> Response {
        Response::Msg(self)
    }
}

impl fmt::Debug for Response {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use Response::*;
        match self {
            Ok => write!(f, "OK"),
            HeartBeat => write!(f, "HEARTBEAT"),
            Msg(m) => write!(f, "{:?}", m),
            Json(s) => write!(f, "{:?}", s),
        }
    }
}
