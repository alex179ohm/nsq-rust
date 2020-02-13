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

use crate::config::ConfigResponse;
use crate::conn;
use crate::error::ClientError;
use crate::handler::MsgHandler;
use crate::handler::Publisher;
use crate::msg::Msg;
//use crate::codec::Message;
use futures::{AsyncRead, AsyncWrite};
use log::debug;
use std::fmt::Display;
//use crossbeam_channel::{Sender, Receiver, unbounded};

pub(crate) struct TcpConnection {
    auth: Option<String>,
    config: ConfigResponse,
    channel: String,
    topic: String,
    rdy: u32,
}

impl Default for TcpConnection {
    fn default() -> TcpConnection {
        TcpConnection {
            auth: None,
            config: ConfigResponse::default(),
            channel: String::new(),
            topic: String::new(),
            rdy: 0u32,
        }
    }
}

#[allow(dead_code)]
impl TcpConnection {
    pub fn new() -> TcpConnection {
        TcpConnection { ..Default::default() }
    }

    pub fn auth(mut self, auth: Option<String>) -> TcpConnection {
        self.auth = auth;
        self
    }

    pub fn config(mut self, cfg: ConfigResponse) -> TcpConnection {
        self.config = cfg;
        self
    }

    pub fn channel(mut self, channel: String, topic: String) -> TcpConnection {
        self.channel = channel;
        self.topic = topic;
        self
    }

    pub fn rdy(mut self, rdy: u32) -> TcpConnection {
        self.rdy = rdy;
        self
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn consume<CHANNEL, TOPIC, S, State>(
    auth: Option<String>,
    config: ConfigResponse,
    stream: &mut conn::NsqStream<'_, S>,
    channel: CHANNEL,
    topic: TOPIC,
    rdy: u32,
    _state: State,
    _future: impl MsgHandler<State>,
) -> Result<(), ClientError>
where
    CHANNEL: Into<String> + Display + Copy,
    TOPIC: Into<String> + Display + Copy,
    S: AsyncRead + AsyncWrite + Unpin,
{
    if config.auth_required {
        conn::authenticate(auth, stream).await?;
    }
    let res = conn::subscribe(stream, channel, topic).await?;
    debug!("SUB {} {}: {:?}", channel, topic, res);
    conn::rdy(stream, rdy).await?;
    debug!("RDY {}", rdy);
    Ok(())
}

pub(crate) async fn publish<S, State>(
    auth: Option<String>,
    config: ConfigResponse,
    stream: &mut conn::NsqStream<'_, S>,
    state: State,
    future: impl Publisher<State>,
) -> Result<Msg, ClientError>
where
    S: AsyncWrite + AsyncRead + Unpin,
{
    if config.auth_required {
        conn::authenticate(auth, stream).await?;
    }
    let msg = future.call(state).await;
    stream.reset();
    conn::publish(stream, msg).await
}
