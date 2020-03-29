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
//ext install TabNine.tabnine-vscode
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

use crate::codec::{Encoder, Sub};
use crate::frame::Frame;
use crate::response::Response;
use futures_io::{AsyncRead, AsyncWrite};
use futures_util::io::{AsyncReadExt, AsyncWriteExt};
use std::convert::TryFrom;
use std::io;

pub async fn subscribe<S: AsyncWrite + AsyncRead + Unpin>(
    stream: &mut S,
    channel: &str,
    topic: &str,
) -> Result<Response, io::Error> {
    stream
        .write_all(&Sub::with_channel_topic(channel, topic).encode()[..])
        .await?;

    let mut buf = [0u8; 1024];

    let size = stream.read(&mut buf).await?;

    match Frame::try_from(&buf[..size]) {
        Ok(Frame::Response(Response::Ok)) => Ok(Response::Ok),
        Ok(r) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{:?}", r),
        )),
        Err(e) => Err(e),
    }
}
