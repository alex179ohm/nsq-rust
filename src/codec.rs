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

use std::ops;
use std::str;

use byteorder::{BigEndian, ByteOrder};

pub struct Message(Vec<u8>);

impl ops::Index<usize> for Message {
    type Output = u8;
    fn index(&self, index: usize) -> &u8 {
        &self.0[index]
    }
}

impl ops::Index<ops::RangeFull> for Message {
    type Output = [u8];
    fn index(&self, _index: ops::RangeFull) -> &[u8] {
        &self.0
    }
}

pub struct Magic;

impl From<Magic> for Message {
    fn from(_: Magic) -> Self {
        Message(Vec::from(&b"  V2"[..]))
    }
}

pub struct Nop;

impl From<Nop> for Message {
    fn from(_: Nop) -> Self {
        Message(Vec::from(&b"NOP\n"[..]))
    }
}

pub struct Identify<'a>(&'a str);

impl<'a> Identify<'a> {
    pub fn new(config: &'a str) -> Self {
        Identify(config)
    }
}

impl From<Identify<'_>> for Message {
    fn from(id: Identify<'_>) -> Self {
        let len = id.0.len();
        let mut buf = Vec::with_capacity(13 + len);
        buf.extend_from_slice(&b"IDENTIFY\n"[..]);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
        buf.extend_from_slice(id.0.as_bytes());
        Message(buf)
    }
}

pub struct Auth<'a>(&'a str);

impl<'a> Auth<'a> {
    pub fn new(auth: &'a str) -> Self {
        Auth(auth)
    }
}

impl From<Auth<'_>> for Message {
    fn from(auth: Auth<'_>) -> Self {
        let len = auth.0.len();
        let mut buf = Vec::with_capacity(9 + len);
        buf.extend_from_slice(&b"AUTH\n"[..]);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
        buf.extend_from_slice(auth.0.as_bytes());
        Message(buf)
    }
}

pub struct Sub<'a>(&'a str, &'a str);

impl<'a> Sub<'a> {
    pub fn new(channel: &'a str, topic: &'a str) -> Self {
        Sub(channel, topic)
    }
}

impl From<Sub<'_>> for Message {
    fn from(sub: Sub<'_>) -> Self {
        let len = sub.0.len();
        let mut buf = Vec::with_capacity(6 + len);
        buf.extend_from_slice(
            &[
                &b"SUB "[..],
                sub.0.as_bytes(),
                &b" "[..],
                sub.1.as_bytes(),
                &b"\n"[..],
            ]
            .concat(),
        );
        Message(buf)
    }
}

pub struct Rdy<'a>(&'a str);

impl<'a> Rdy<'a> {
    pub fn new(rdy: &'a str) -> Self {
        Rdy(rdy)
    }
}

impl From<Rdy<'_>> for Message {
    fn from(rdy: Rdy<'_>) -> Self {
        let mut buf = Vec::with_capacity(5 + rdy.0.len());
        buf.extend_from_slice(&[&b"RDY "[..], rdy.0.as_bytes(), &b"\n"[..]].concat());
        Message(buf)
    }
}

pub struct Pub(String, Vec<u8>);

impl Pub {
    pub fn new(topic: String, msg: Vec<u8>) -> Self {
        Pub(topic, msg)
    }
}

impl From<Pub> for Message {
    fn from(pb: Pub) -> Self {
        let msg_len = pb.1.len();
        let len = pb.0.len() + msg_len;
        let mut buf = Vec::with_capacity(9 + len);
        buf.extend_from_slice(&[&b"PUB "[..], pb.0.as_bytes(), &b"\n"[..]].concat());
        buf.extend_from_slice(&(msg_len as u32).to_be_bytes());
        buf.extend_from_slice(pb.1.as_slice());
        Message(buf)
    }
}

pub struct Mpub(String, Vec<Vec<u8>>);

impl Mpub {
    pub fn new(topic: String, msgs: Vec<Vec<u8>>) -> Mpub {
        Mpub(topic, msgs)
    }
}

impl From<Mpub> for Message {
    fn from(mpub: Mpub) -> Self {
        let num_msgs = mpub.1.len();
        let total_msgs_len = mpub.1.iter().fold(0, |acc, e| acc + e.len() + 4);
        let len = mpub.0.len();
        let mut buf = Vec::with_capacity(14 + len + total_msgs_len);
        buf.extend_from_slice(&[&b"MPUB "[..], mpub.0.as_bytes(), &b"\n"[..]].concat());
        buf.extend_from_slice(&(total_msgs_len as u32).to_be_bytes());
        buf.extend_from_slice(&(num_msgs as u32).to_be_bytes());
        for msg in mpub.1 {
            buf.extend_from_slice(&(msg.len() as u32).to_be_bytes());
            buf.extend_from_slice(msg.as_slice());
        }
        Message(buf)
    }
}

pub struct Dpub(String, String, Vec<u8>);

impl Dpub {
    pub fn new(topic: String, time: u32, msg: Vec<u8>) -> Dpub {
        Dpub(topic, time.to_string(), msg)
    }
}

impl From<Dpub> for Message {
    fn from(msg: Dpub) -> Self {
        let msg_len = msg.2.len();
        let len = msg.0.len() + msg.1.len() + msg_len;
        let mut buf = Vec::with_capacity(11 + len);
        buf.extend_from_slice(
            &[
                &b"DPUB "[..],
                msg.0.as_bytes(),
                &b" "[..],
                msg.1.as_bytes(),
                &b"\n"[..],
            ]
            .concat(),
        );
        buf.extend_from_slice(&(msg_len as u32).to_be_bytes());
        buf.extend_from_slice(msg.2.as_slice());
        Message(buf)
    }
}

pub struct Touch<'a>(&'a str);

impl<'a> Touch<'a> {
    pub fn new(id: &'a str) -> Self {
        Touch(id)
    }
}

impl From<Touch<'_>> for Message {
    fn from(touch: Touch) -> Self {
        let mut buf = Vec::with_capacity(touch.0.len() + 7);
        buf.extend_from_slice(&[&b"TOUCH "[..], touch.0.as_bytes(), &b"\n"[..]].concat());
        Message(buf)
    }
}

pub struct Fin<'a>(&'a str);

impl<'a> Fin<'a> {
    pub fn new(id: &'a str) -> Self {
        Fin(id)
    }
}

impl From<Fin<'_>> for Message {
    fn from(fin: Fin) -> Self {
        let mut buf = Vec::with_capacity(fin.0.len() + 5);
        buf.extend_from_slice(&[&b"FIN "[..], fin.0.as_bytes(), &b"\n"[..]].concat());
        Message(buf)
    }
}

pub struct Req<'a>(&'a str, u32);

impl<'a> Req<'a> {
    pub fn new(id: &'a str, timeout: u32) -> Self {
        Req(id, timeout)
    }
}

impl From<Req<'_>> for Message {
    fn from(req: Req) -> Self {
        let timeout = req.1.to_string();
        let mut buf = Vec::with_capacity(req.0.len() + timeout.len() + 6);
        buf.extend_from_slice(
            &[
                &b"REQ "[..],
                req.0.as_bytes(),
                &b" "[..],
                timeout.as_bytes(),
                &b"\n"[..],
            ]
            .concat(),
        );
        Message(buf)
    }
}

pub struct Cls;

impl From<Cls> for Message {
    fn from(_: Cls) -> Self {
        Message(Vec::from(&b"CLS\n"[..]))
    }
}

pub fn decode_msg(buf: &mut [u8]) -> (i64, u16, String, Vec<u8>) {
    let timestamp = BigEndian::read_i64(&buf[..8]);
    let attemps = BigEndian::read_u16(&buf[8..10]);
    let id_bytes = &buf[10..26];
    let id = str::from_utf8(id_bytes).expect("failed to decode id message");
    (timestamp, attemps, id.to_owned(), Vec::from(&buf[26..]))
}
