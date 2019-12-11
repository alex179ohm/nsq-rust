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

use crate::codec::{Auth, Encoder, Identify, Magic, Sub, Rdy};
use crate::config::Config;
use crate::error::NsqError;
use crate::response::Response;
use crate::result::NsqResult;
use async_std::io::prelude::*;
use async_std::stream::StreamExt;
use bytes::BytesMut;
use futures::Stream;

pub(crate) async fn magic<IO: Write + Unpin>(io: &mut IO, buf: &mut BytesMut) -> NsqResult<()> {
    Magic {}.encode(buf);
    if let Err(e) = io.write_all(&buf.take()[..]).await {
        return Err(NsqError::from(e));
    };
    Ok(())
}

pub(crate) async fn identify<IO: Write + Stream<Item = NsqResult<Response>> + Unpin>(
    io: &mut IO,
    config: Config,
    buf: &mut BytesMut,
) -> NsqResult<Response> {
    if let Ok(msg_string) = serde_json::to_string(&config) {
        Identify::new(msg_string.as_str()).encode(buf);
    };
    if let Err(e) = io.write_all(&buf.take()[..]).await {
        return Err(NsqError::from(e));
    };
    io.next().await.unwrap()
}

pub(crate) async fn auth<IO, AUTH>(
    io: &mut IO,
    auth: AUTH,
    buf: &mut BytesMut,
) -> NsqResult<Response>
where
    IO: Write + Stream<Item = NsqResult<Response>> + Unpin,
    AUTH: Into<String>,
{
    Auth::new(auth.into().as_str()).encode(buf);
    if let Err(e) = io.write_all(&buf.take()[..]).await {
        return Err(NsqError::from(e));
    };
    io.next().await.unwrap()
}

pub(crate) async fn sub<IO, CHANNEL, TOPIC>(
    io: &mut IO,
    buf: &mut BytesMut,
    channel: CHANNEL,
    topic: TOPIC,
) -> NsqResult<Response>
where
    IO: Write + Stream<Item = NsqResult<Response>> + Unpin,
    CHANNEL: Into<String>,
    TOPIC: Into<String>,
{
    Sub::new(&channel.into(), &topic.into()).encode(buf);
    if let Err(e) = io.write_all(&buf.take()[..]).await {
        return Err(NsqError::from(e));
    }
    io.next().await.unwrap()
}

pub(crate) async fn rdy<IO: Write + Unpin>(io: &mut IO, buf: &mut BytesMut, rdy: u32) -> NsqResult<()> {
    Rdy::new(&rdy.to_string()).encode(buf);
    if let Err(e) = io.write_all(&buf.take()[..]).await {
        return Err(NsqError::from(e))
    }
    Ok(())
}