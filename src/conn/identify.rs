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

use crate::codec::{Identify, Message};
use crate::config::Config;
use crate::error::ClientError;
use crate::msg::Msg;
use async_std::prelude::*;
use futures::{AsyncWrite, Stream};

/// Send the [Identify](struct.Identify.html) *msg* to nsqd, and return the
pub async fn identify<IO>(io: &mut IO, config: Config) -> Result<Msg, ClientError>
where
    IO: AsyncWrite + Stream<Item = Result<Msg, ClientError>> + Unpin,
{
    let msg_string = serde_json::to_string(&config)?;
    let buf: Message = Identify::new(msg_string.as_str()).into();

    if let Err(e) = io.write_all(&buf[..]).await {
        return Err(ClientError::from(e));
    };

    io.next().await.unwrap()
}
