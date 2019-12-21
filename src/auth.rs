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

use serde::{Deserialize, Serialize};
use crate::result::NsqResult;
use futures::{AsyncRead, AsyncWrite};
use crate::utils;
use crate::io::NsqIO;
use crate::msg::Msg;
use std::io;
use log::debug;

/// Authentication response sent by nsqd to the client after the AUTH command.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Reply {
    identity: String,
    identity_url: Option<String>,
    permission_count: i32,
}

pub(crate) async fn authenticate<S: AsyncRead + AsyncWrite + Unpin>(auth: Option<String>, stream: &mut NsqIO<'_, S>) -> NsqResult<()> {
    if auth.is_none() {
        return Err(io::Error::new(io::ErrorKind::Other, "When configured with auth, authentication token must be provided").into())
    }
    let auth_token = auth.unwrap();
    stream.reset();
    if let Msg::Json(s) = utils::auth(stream, auth_token).await? {
        let auth: Reply = serde_json::from_str(&s)?;
        debug!("AUTH: {:?}", auth);
    }
    Ok(())
}
