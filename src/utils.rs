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

use crate::codec::{Auth, Identify, Magic, Message, Rdy, Sub};
use crate::config::Config;
use crate::error::NsqError;
use crate::msg::Msg;
use crate::result::NsqResult;
use async_std::io::prelude::*;
use async_std::stream::StreamExt;
use futures::Stream;

pub(crate) async fn magic<IO: Write + Unpin>(io: &mut IO) -> NsqResult<()> {
    let buf: Message = Magic {}.into();
    if let Err(e) = io.write_all(buf.as_slice()).await {
        return Err(NsqError::from(e));
    };
    Ok(())
}

pub(crate) async fn identify<IO: Write + Stream<Item = NsqResult<Msg>> + Unpin>(
    io: &mut IO,
    config: Config,
) -> NsqResult<Msg> {
    let msg_string = serde_json::to_string(&config)?;
    let buf: Message = Identify::new(msg_string.as_str()).into();
    if let Err(e) = io.write_all(buf.as_slice()).await {
        return Err(NsqError::from(e));
    };
    io.next().await.unwrap()
}

pub(crate) async fn auth<IO, AUTH>(io: &mut IO, auth: AUTH) -> NsqResult<Msg>
where
    IO: Write + Stream<Item = NsqResult<Msg>> + Unpin,
    AUTH: Into<String>,
{
    let buf: Message = Auth::new(auth.into().as_str()).into();
    if let Err(e) = io.write_all(buf.as_slice()).await {
        return Err(NsqError::from(e));
    };
    io.next().await.unwrap()
}

pub(crate) async fn sub<IO, CHANNEL, TOPIC>(
    io: &mut IO,
    channel: CHANNEL,
    topic: TOPIC,
) -> NsqResult<Msg>
where
    IO: Write + Stream<Item = NsqResult<Msg>> + Unpin,
    CHANNEL: Into<String>,
    TOPIC: Into<String>,
{
    let buf: Message = Sub::new(&channel.into(), &topic.into()).into();
    if let Err(e) = io.write_all(buf.as_slice()).await {
        return Err(NsqError::from(e));
    }
    io.next().await.unwrap()
}

pub(crate) async fn rdy<IO: Write + Unpin>(io: &mut IO, rdy: u32) -> NsqResult<()> {
    let buf: Message = Rdy::new(&rdy.to_string()).into();
    if let Err(e) = io.write_all(buf.as_slice()).await {
        return Err(NsqError::from(e));
    }
    Ok(())
}

pub async fn io_publish<S>(io: &mut S, msg: Message) -> NsqResult<Msg>
where
    S: Write + Stream<Item = NsqResult<Msg>> + Unpin,
{
    if let Err(e) = io.write_all(msg.as_slice()).await {
        return Err(NsqError::from(e));
    }
    io.next().await.unwrap()
}
