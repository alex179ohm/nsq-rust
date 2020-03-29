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

use std::io;
use std::str;

use byteorder::{BigEndian, ByteOrder};
use bytes::BytesMut;

pub trait Encoder {
    fn encode(&self) -> BytesMut;
}

pub trait Decoder
where
    Self: Sized,
{
    type Error;
    fn decode(buf: &[u8]) -> Result<Self, Self::Error>;
}

pub struct Magic;

impl Encoder for Magic {
    fn encode(&self) -> BytesMut {
        BytesMut::from(&b"  V2"[..])
    }
}

pub struct Nop;

impl Encoder for Nop {
    fn encode(&self) -> BytesMut {
        BytesMut::from(&b"NOP\n"[..])
    }
}

pub struct Identify<'a>(&'a str);

impl<'a> Identify<'a> {
    pub fn with_config(config: &'a str) -> Self {
        Identify(config)
    }
}

impl Encoder for Identify<'_> {
    fn encode(&self) -> BytesMut {
        let len = self.0.len();
        let mut buf = BytesMut::with_capacity(13 + len);
        buf.extend_from_slice(&b"IDENTIFY\n"[..]);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
        buf.extend_from_slice(self.0.as_bytes());
        buf
    }
}

pub struct Auth<'a>(&'a str);

impl<'a> Auth<'a> {
    pub fn with_auth(auth: &'a str) -> Self {
        Auth(auth)
    }
}

impl Encoder for Auth<'_> {
    fn encode(&self) -> BytesMut {
        let len = self.0.len();
        let mut buf = BytesMut::with_capacity(9 + len);
        buf.extend_from_slice(&b"AUTH\n"[..]);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
        buf.extend_from_slice(self.0.as_bytes());
        buf
    }
}

pub struct Sub<'a>(&'a str, &'a str);

impl<'a> Sub<'a> {
    pub fn with_channel_topic(channel: &'a str, topic: &'a str) -> Self {
        Sub(channel, topic)
    }
}

impl Encoder for Sub<'_> {
    fn encode(&self) -> BytesMut {
        let len = self.0.len();
        let mut buf = BytesMut::with_capacity(6 + len);
        buf.extend_from_slice(
            &[
                &b"SUB "[..],
                self.0.as_bytes(),
                &b" "[..],
                self.1.as_bytes(),
                &b"\n"[..],
            ]
            .concat(),
        );
        buf
    }
}

pub struct Rdy<'a>(&'a str);

impl<'a> Rdy<'a> {
    pub fn with_str(s: &'a str) -> Rdy<'a> {
        Rdy(s)
    }
}

impl Encoder for Rdy<'_> {
    fn encode(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(5 + self.0.len());
        buf.extend_from_slice(&[&b"RDY "[..], self.0.as_bytes(), &b"\n"[..]].concat());
        buf
    }
}

pub struct Pub<'a>(&'a str, &'a [u8]);

impl<'a> Pub<'a> {
    pub fn with_topic_msg(topic: &'a str, msg: &'a [u8]) -> Self {
        Pub(topic, msg)
    }
}

impl Encoder for Pub<'_> {
    fn encode(&self) -> BytesMut {
        let msg_len = self.1.len();
        let len = self.0.len() + msg_len;
        let mut buf = BytesMut::with_capacity(9 + len);
        buf.extend_from_slice(&[&b"PUB "[..], self.0.as_bytes(), &b"\n"[..]].concat());
        buf.extend_from_slice(&(msg_len as u32).to_be_bytes());
        buf.extend_from_slice(&self.1[..]);
        buf
    }
}

pub struct Mpub<'a>(&'a str, &'a [&'a [u8]]);

impl<'a> Mpub<'a> {
    pub fn with_topic_msgs(topic: &'a str, msgs: &'a [&'a [u8]]) -> Mpub<'a> {
        Mpub(topic, msgs)
    }
}

impl Encoder for Mpub<'_> {
    fn encode(&self) -> BytesMut {
        let num_msgs = self.1.len();
        let total_msgs_len = self.1.iter().fold(0, |acc, e| acc + e.len() + 4);
        let len = self.0.len();
        let mut buf = BytesMut::with_capacity(14 + len + total_msgs_len);
        buf.extend_from_slice(&[&b"MPUB "[..], self.0.as_bytes(), &b"\n"[..]].concat());
        buf.extend_from_slice(&(total_msgs_len as u32).to_be_bytes());
        buf.extend_from_slice(&(num_msgs as u32).to_be_bytes());
        for msg in self.1 {
            buf.extend_from_slice(&(msg.len() as u32).to_be_bytes());
            buf.extend_from_slice(&msg[..]);
        }
        buf
    }
}

pub struct Dpub<'a>(&'a str, u32, &'a [u8]);

impl<'a> Dpub<'a> {
    pub fn with_topic_time_msg(topic: &'a str, time: u32, msg: &'a [u8]) -> Dpub<'a> {
        Dpub(topic, time, msg)
    }
}

impl Encoder for Dpub<'_> {
    fn encode(&self) -> BytesMut {
        let time = self.1.to_string();
        let msg_len = self.2.len();
        let len = self.0.len() + time.len() + msg_len;
        let mut buf = BytesMut::with_capacity(11 + len);
        buf.extend_from_slice(
            &[
                &b"DPUB "[..],
                self.0.as_bytes(),
                &b" "[..],
                time.as_bytes(),
                &b"\n"[..],
            ]
            .concat(),
        );
        buf.extend_from_slice(&(msg_len as u32).to_be_bytes());
        buf.extend_from_slice(&self.2[..]);
        buf
    }
}

pub struct Touch<'a>(&'a str);

impl<'a> From<&'a str> for Touch<'a> {
    fn from(id: &'a str) -> Self {
        Touch::with_id(id)
    }
}

impl<'a> Touch<'a> {
    pub fn with_id(id: &'a str) -> Self {
        Touch(id)
    }
}

impl Encoder for Touch<'_> {
    fn encode(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(self.0.len() + 7);
        buf.extend_from_slice(&[&b"TOUCH "[..], self.0.as_bytes(), &b"\n"[..]].concat());
        buf
    }
}

pub struct Fin<'a>(&'a str);

impl<'a> From<&'a str> for Fin<'a> {
    fn from(id: &'a str) -> Self {
        Fin::with_id(id)
    }
}

impl<'a> Fin<'a> {
    pub fn with_id(id: &'a str) -> Self {
        Fin(id)
    }
}

impl Encoder for Fin<'_> {
    fn encode(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(self.0.len() + 5);
        buf.extend_from_slice(&[&b"FIN "[..], self.0.as_bytes(), &b"\n"[..]].concat());
        buf
    }
}

pub struct Req<'a>(&'a str, u32);

impl<'a> From<(&'a str, u32)> for Req<'a> {
    fn from(id_timeout: (&'a str, u32)) -> Self {
        Req::with_id_timeout(id_timeout.0, id_timeout.1)
    }
}

impl<'a> Req<'a> {
    pub fn with_id_timeout(id: &'a str, timeout: u32) -> Self {
        Req(id, timeout)
    }
}

impl Encoder for Req<'_> {
    fn encode(&self) -> BytesMut {
        let timeout = self.1.to_string();
        let mut buf = BytesMut::with_capacity(self.0.len() + timeout.len() + 6);
        buf.extend_from_slice(
            &[
                &b"REQ "[..],
                self.0.as_bytes(),
                &b" "[..],
                timeout.as_bytes(),
                &b"\n"[..],
            ]
            .concat(),
        );
        buf
    }
}

pub struct Cls;

impl Encoder for Cls {
    fn encode(&self) -> BytesMut {
        BytesMut::from(&b"CLS\n"[..])
    }
}

pub fn decode_msg(buf: &[u8]) -> Result<(i64, u16, String, Vec<u8>), io::Error> {
    if buf.len() < 26 {
        log::debug!("{:?}", buf);
        io::Error::new(io::ErrorKind::InvalidData, "msg size is less than Header");
    }
    let timestamp = BigEndian::read_i64(&buf[..8]);
    let attemps = BigEndian::read_u16(&buf[8..10]);
    let id = str::from_utf8(&buf[10..26])
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("{:?}", e)))?;
    Ok((timestamp, attemps, id.to_owned(), Vec::from(&buf[26..])))
}
