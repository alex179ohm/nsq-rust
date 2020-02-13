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

use super::NsqStream;
use crate::auth;
use crate::config::ConfigResponse;
use crate::error::ClientError;
use crate::handler::Consumer;
use crate::handler::Publisher;
use crate::msg::Msg;
use crate::utils;
use futures::{AsyncRead, AsyncWrite};
use log::debug;
use std::fmt::Display;

#[allow(clippy::too_many_arguments)]
pub(crate) async fn consume<CHANNEL, TOPIC, S, State>(
    auth: Option<String>,
    config: ConfigResponse,
    stream: &mut NsqStream<'_, S>,
    channel: CHANNEL,
    topic: TOPIC,
    rdy: u32,
    _state: State,
    _future: impl Consumer<State>,
) -> Result<(), ClientError>
where
    CHANNEL: Into<String> + Display + Copy,
    TOPIC: Into<String> + Display + Copy,
    S: AsyncRead + AsyncWrite + Unpin,
{
    if config.auth_required {
        auth::authenticate(auth, stream).await?;
    }
    let res = utils::sub(stream, channel, topic).await?;
    debug!("SUB {} {}: {:?}", channel, topic, res);
    utils::rdy(stream, rdy).await?;
    debug!("RDY {}", rdy);
    Ok(())
}

pub(crate) async fn publish<S, State>(
    auth: Option<String>,
    config: ConfigResponse,
    stream: &mut NsqStream<'_, S>,
    state: State,
    future: impl Publisher<State>,
) -> Result<Msg, ClientError>
where
    S: AsyncWrite + AsyncRead + Unpin,
{
    if config.auth_required {
        auth::authenticate(auth, stream).await?;
    }
    let msg = future.call(state).await;
    stream.reset();
    utils::io_publish(stream, msg).await
}
