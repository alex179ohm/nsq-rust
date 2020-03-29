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

use crate::codec::{Auth, Encoder};
use crate::frame::Frame;
use crate::response::Response;
use async_std::prelude::*;
use futures_io::{AsyncRead, AsyncWrite};
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::io;

/// The authentication response received by nsqd.
///
/// Version required: nsqd v0.2.19+
///
/// As described by the [Nsq protocol specification](https://nsq.io/clients/tcp_protocol_spec.html#auth)
/// it contains the client's identity, an optional url and a count of permission which were
/// authorized by the eventually external authentication service.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AuthResponse {
    /// client's identity received by nsqd Json success response.
    pub identity: String,
    /// Optional identity url received by nsqd Json success response to AUTH message.
    pub identity_url: Option<String>,
    /// Permission_count received by nsqd Json success response to AUTH message.
    ///
    /// Even if not specified by both [nsqd protocol specification](https://nsq.io/clients/tcp_protocol_spec.html#auth)
    /// or the [nsqd documentation](https://nsq.io/clients/tcp_protocol_spec.html#auth) it is
    /// supposed that permission_count is just a merely count of permissions granted by the
    /// eventually external authentication service.
    ///
    /// Simply like:
    /// - permissions_granted: [subscribe, publish] -> permission_count: 2
    /// - permissions_granted: [subscribe] -> permission_count: 1
    pub permission_count: i32,
}

/// Synchronously sends the [AUTH](https://nsq.io/clients/tcp_protocol_spec.html#auth) message
/// and waits for the nsqd response.
///
/// This function is just called if the client and nsqd are both configured to performs
/// authentication.
///
/// Even if nsqd responds with and error or with an invalid response the error returned is a
/// [ClientError::Io](enum.ClientError.html).
///
/// Obiuvsly on success it returns [AuthResponse](struct.AuthResponse.html).
///
/// # Example
/// ```no-run
/// match authenticate(Some(auth_string), nsq_io_stream) {
///     Ok(msg) => println!("{}", msg),
///     Err(e) => eprintln!("{}", e),
/// }
/// ```
pub async fn authenticate<S: AsyncRead + AsyncWrite + Unpin>(
    auth_token: &str,
    stream: &mut S,
) -> Result<AuthResponse, Box<dyn std::error::Error>> {
    stream
        .write_all(&Auth::with_auth(auth_token).encode()[..])
        .await?;

    let mut buf = [0u8; 1024];

    let size = stream.read(&mut buf).await?;

    if let Ok(Frame::Response(Response::Json(s))) = Frame::try_from(&buf[..size]) {
        let auth: AuthResponse = serde_json::from_str(&s)?;
        Ok(auth)
    } else {
        Err(Box::new(io::Error::new(
            io::ErrorKind::Other,
            "unsupported response",
        )))
    }
}
