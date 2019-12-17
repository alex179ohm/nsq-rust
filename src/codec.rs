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

use bytes::{BufMut, BytesMut};
use std::str;

use byteorder::{BigEndian, ByteOrder};

pub struct Message(BytesMut);

impl Message {
    pub fn as_bytes_mut(&self) -> &BytesMut {
        &self.0
    }
}

/// The Encoder trait allow to encode different kind of commands into the BytesMut buffer.
pub trait Encoder {
    fn encode(self, buf: &mut BytesMut);
}

pub struct Magic;

impl Encoder for Magic {
    fn encode(self, buf: &mut BytesMut) {
        buf.put(&b"  V2"[..]);
    }
}

impl From<Magic> for Message {
    fn from(_: Magic) -> Self {
        let mut buf = BytesMut::with_capacity(4);
        buf.put(&b"  V2"[..]);
        Message(buf)
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
        let mut buf = BytesMut::with_capacity(13 + len);
        buf.put(&b"IDENTIFY\n"[..]);
        buf.put_u32_be(len as u32);
        buf.put(id.0.as_bytes());
        Message(buf)
    }
}

impl<'a> Encoder for Identify<'a> {
    fn encode(self, buf: &mut BytesMut) {
        let len = self.0.len();
        check_and_reserve(buf, 13 + len);
        buf.put(&b"IDENTIFY\n"[..]);
        buf.put_u32_be(len as u32);
        buf.put(self.0.as_bytes());
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
        let mut buf = BytesMut::with_capacity(9 + len);
        buf.put(&b"AUTH\n"[..]);
        buf.put_u32_be(len as u32);
        buf.put(auth.0.as_bytes());
        Message(buf)
    }
}

impl<'a> Encoder for Auth<'a> {
    fn encode(self, buf: &mut BytesMut) {
        let len = self.0.len();
        check_and_reserve(buf, 9 + len);
        buf.put(&b"AUTH\n"[..]);
        buf.put_u32_be(len as u32);
        buf.put(self.0.as_bytes());
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
        let mut buf = BytesMut::with_capacity(6 + len);
        buf.put(&b"SUB "[..]);
        buf.put(sub.0.as_bytes());
        buf.put(&b" "[..]);
        buf.put(sub.1.as_bytes());
        buf.put(&b"\n"[..]);
        Message(buf)
    }
}

impl<'a> Encoder for Sub<'a> {
    fn encode(self, buf: &mut BytesMut) {
        let len = self.0.len() + self.1.len();
        check_and_reserve(buf, 6 + len);
        buf.put(&b"SUB "[..]);
        buf.put(self.0.as_bytes());
        buf.put(&b" "[..]);
        buf.put(self.1.as_bytes());
        buf.put(&b"\n"[..]);
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
        let mut buf = BytesMut::with_capacity(5 + rdy.0.len());
        buf.put(&b"RDY "[..]);
        buf.put(rdy.0.as_bytes());
        buf.put(&b"\n"[..]);
        Message(buf)
    }
}

impl Encoder for Rdy<'_> {
    fn encode(self, buf: &mut BytesMut) {
        check_and_reserve(buf, self.0.len() + 5);
        buf.put(&b"RDY "[..]);
        buf.put(self.0.as_bytes());
        buf.put(&b"\n"[..]);
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
        let mut buf = BytesMut::with_capacity(9 + len);
        buf.put(&b"PUB "[..]);
        buf.put(pb.0.as_bytes());
        buf.put(&b"\n"[..]);
        buf.put_u32_be(msg_len as u32);
        buf.put(pb.1.as_slice());
        Message(buf)
    }
}

impl Encoder for Pub {
    fn encode(self, buf: &mut BytesMut) {
        let msg_len = self.1.len();
        let len = self.0.len() + msg_len;
        check_and_reserve(buf, 9 + len);
        buf.put(&b"PUB "[..]);
        buf.put(self.0.as_bytes());
        buf.put(&b"\n"[..]);
        buf.put_u32_be(msg_len as u32);
        buf.put(self.1.as_slice());
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
        let mut buf = BytesMut::with_capacity(14 + len + total_msgs_len);
        buf.put(&b"MPUB "[..]);
        buf.put(mpub.0.as_bytes());
        buf.put(&b"\n"[..]);
        buf.put_u32_be(total_msgs_len as u32);
        buf.put_u32_be(num_msgs as u32);
        for msg in mpub.1 {
            buf.put_u32_be(msg.len() as u32);
            buf.put(msg);
        }
        Message(buf)
    }
}

impl Encoder for Mpub {
    fn encode(self, buf: &mut BytesMut) {
        let num_msgs = self.1.len();
        let total_msgs_len = self.1.iter().fold(0, |acc, e| acc + e.len() + 4);
        let len = self.0.len();
        check_and_reserve(buf, 14 + len + total_msgs_len);
        buf.put(&b"MPUB "[..]);
        buf.put(self.0.as_bytes());
        buf.put(&b"\n"[..]);
        buf.put_u32_be(total_msgs_len as u32);
        buf.put_u32_be(num_msgs as u32);
        for msg in self.1 {
            buf.put_u32_be(msg.len() as u32);
            buf.put(msg);
        }
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
        let mut buf = BytesMut::with_capacity(11 + len);
        buf.put(&b"DPUB "[..]);
        buf.put(msg.0.as_bytes());
        buf.put(&b" "[..]);
        buf.put(msg.1.as_bytes());
        buf.put(&b"\n"[..]);
        buf.put_u32_be(msg_len as u32);
        buf.put(msg.2.as_slice());
        Message(buf)
    }
}

impl Encoder for Dpub {
    fn encode(self, buf: &mut BytesMut) {
        let msg_len = self.2.len();
        let len = self.0.len() + self.1.len() + msg_len;
        check_and_reserve(buf, 11 + len);
        buf.put(&b"DPUB "[..]);
        buf.put(self.0.as_bytes());
        buf.put(&b" "[..]);
        buf.put(self.1.as_bytes());
        buf.put(&b"\n"[..]);
        buf.put_u32_be(msg_len as u32);
        buf.put(self.2.as_slice());
    }
}

pub struct Touch(String);

impl Touch {
    pub fn new(id: String) -> Self {
        Touch(id)
    }
}

impl From<Touch> for Message {
    fn from(touch: Touch) -> Self {
        let mut buf = BytesMut::with_capacity(touch.0.len() + 7);
        buf.put(&b"TOUCH "[..]);
        buf.put(touch.0.as_bytes());
        buf.put(&b"\n"[..]);
        Message(buf)
    }
}

impl Encoder for Touch {
    fn encode(self, buf: &mut BytesMut) {
        check_and_reserve(buf, self.0.len() + 7);
        buf.put(&b"TOUCH "[..]);
        buf.put(self.0.as_bytes());
        buf.put(&b"\n"[..]);
    }
}

pub struct Fin(String);

impl Fin {
    pub fn new(id: String) -> Self {
        Fin(id)
    }
}

impl From<Fin> for Message {
    fn from(fin: Fin) -> Self {
        let mut buf = BytesMut::with_capacity(fin.0.len() + 5);
        buf.put(&b"FIN "[..]);
        buf.put(fin.0.as_bytes());
        buf.put(&b"\n"[..]);
        Message(buf)
    }
}

impl Encoder for Fin {
    fn encode(self, buf: &mut BytesMut) {
        check_and_reserve(buf, self.0.len() + 5);
        buf.put(&b"FIN "[..]);
        buf.put(self.0.as_bytes());
        buf.put(&b"\n"[..]);
    }
}

pub struct Req(String, String);

impl Req {
    pub fn new(id: String, timeout: u32) -> Self {
        Req(id, timeout.to_string())
    }
}

impl From<Req> for Message {
    fn from(req: Req) -> Self {
        let mut buf = BytesMut::with_capacity(req.0.len() + req.1.len() + 6);
        buf.put(&b"REQ "[..]);
        buf.put(req.0.as_bytes());
        buf.put(&b" "[..]);
        buf.put(req.1.as_bytes());
        buf.put(&b"\n"[..]);
        Message(buf)
    }
}

impl Encoder for Req {
    fn encode(self, buf: &mut BytesMut) {
        check_and_reserve(buf, self.0.len() + self.1.len() + 6);
        buf.put(&b"REQ "[..]);
        buf.put(self.0.as_bytes());
        buf.put(&b" "[..]);
        buf.put(self.1.as_bytes());
        buf.put(&b"\n"[..]);
    }
}

pub struct Cls;

impl From<Cls> for Message {
    fn from(_: Cls) -> Self {
        let mut buf = BytesMut::with_capacity(4);
        buf.put(&b"CLS\n"[..]);
        Message(buf)
    }
}

impl Encoder for Cls {
    fn encode(self, buf: &mut BytesMut) {
        check_and_reserve(buf, 4);
        buf.put(&b"CLS\n"[..]);
    }
}

pub fn decode_msg(buf: &mut [u8]) -> (i64, u16, String, Vec<u8>) {
    // skip size and frame type
    let timestamp = BigEndian::read_i64(&buf[..8]);
    let attemps = BigEndian::read_u16(&buf[8..10]);
    let id_bytes = &buf[10..26];
    let id = str::from_utf8(id_bytes).expect("failed to decode id message");
    (timestamp, attemps, id.to_owned(), Vec::from(&buf[26..]))
}

fn check_and_reserve(buf: &mut BytesMut, size: usize) {
    if buf.remaining_mut() < size {
        buf.reserve(size);
    }
}
