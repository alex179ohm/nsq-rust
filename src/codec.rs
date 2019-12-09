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

use bytes::{BufMut, BytesMut};
use std::str;

use byteorder::{BigEndian, ByteOrder};

pub trait Encoder {
    fn encode(self, buf: &mut BytesMut);
}

pub trait Decoder {
    fn decode<MSG: Message>(buf: BytesMut) -> MSG;
}

pub trait Message {}

pub struct Magic;

impl Encoder for Magic {
    fn encode(self, buf: &mut BytesMut) {
        buf.put(&b"  V2"[..]);
    }
}

//pub struct Cls;
//
//impl Encoder for Cls {
//    fn encode(self, buf: &mut BytesMut) {
//        buf.put(&b"CLS\n"[..]);
//    }
//}
//
pub struct Identify<'a>(&'a str);

impl<'a> Identify<'a> {
    pub fn new(config: &'a str) -> Self {
        Identify(config)
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

pub struct Pub(String, Vec<u8>);

impl Pub {
    pub fn new(topic: String, msg: Vec<u8>) -> Self {
        Pub(topic, msg)
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

impl Encoder for Mpub {
    fn encode(self, buf: &mut BytesMut) {
        let num_msgs = self.1.len();
        let total_msgs_len = self.1.iter().fold(0, |acc, e| { acc + e.len() + 4 });
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

//pub struct Rdy<'a>(&'a str);
//
//impl<'a> Rdy<'a> {
//    pub fn new(n: &'a str) -> Self {
//        Rdy(n)
//    }
//}

//impl<'a> Encoder for Rdy<'a> {
//    fn encode(self, buf: &mut BytesMut) {
//        let len = self.0.len();
//        check_and_reserve(buf, 5 + len);
//        buf.put(&b"RDY "[..]);
//        buf.put(self.0.as_bytes());
//        buf.put(&b"\n"[..]);
//    }
//}

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
