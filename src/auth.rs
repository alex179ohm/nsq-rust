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

use crate::conn;
use crate::error::ClientError;
use crate::msg::Msg;
use futures::{AsyncRead, AsyncWrite};
use log::debug;
use serde::{Deserialize, Serialize};
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
pub(crate) async fn authenticate<S: AsyncRead + AsyncWrite + Unpin>(
    auth: Option<String>,
    stream: &mut conn::NsqStream<'_, S>,
) -> Result<AuthResponse, ClientError> {
    if auth.is_none() {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            "When configured with auth, authentication token must be provided",
        )
        .into());
    };

    let auth_token = auth.unwrap();
    stream.reset();

    match conn::auth(stream, auth_token).await? {
        Msg::Json(s) => {
            let auth: AuthResponse = serde_json::from_str(&s)?;
            debug!("AUTH: {:?}", auth);
            return Ok(auth);
        }
        s => {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("AUTH msg received an invalid response: {:?}", s),
            )
            .into());
        }
    }
}
