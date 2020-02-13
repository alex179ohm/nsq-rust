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

use crate::auth;
use crate::conn;
use crate::config::ConfigResponse;
use crate::error::ClientError;
use crate::handler::Consumer;
use crate::handler::Publisher;
use crate::msg::Msg;
use async_std::net::TcpStream;
use async_std::stream::StreamExt;
use async_tls::TlsConnector;
use log::debug;
use rustls::ClientConfig;
use std::fmt::Display;
use std::io;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[allow(clippy::too_many_arguments)]
pub(crate) async fn consume<CHANNEL, TOPIC, State>(
    addr: String,
    auth: Option<String>,
    config: ConfigResponse,
    cafile: Option<PathBuf>,
    stream: &mut TcpStream,
    channel: CHANNEL,
    topic: TOPIC,
    rdy: u32,
    _future: impl Consumer<State>,
) -> Result<(), ClientError>
where
    CHANNEL: Into<String> + Copy + Display,
    TOPIC: Into<String> + Copy + Display,
{
    let addr: Vec<&str> = addr.split(':').collect();
    let connector = if let Some(cafile) = cafile {
        connector_from_cafile(&cafile)
            .await
            .expect("failed to read cafile")
    } else {
        TlsConnector::new()
    };
    let mut tls_stream = connector.connect(addr[0], stream).unwrap().await?;
    let mut stream = conn::NsqStream::new(&mut tls_stream, 1024);
    stream.next().await.unwrap()?;
    debug!("TLS Ok");
    if config.auth_required {
        auth::authenticate(auth, &mut stream).await?;
    }
    let res = conn::subscribe(&mut stream, channel, topic).await?;
    debug!("SUB: {} {}: {:?}", channel, topic, res);
    conn::rdy(&mut stream, rdy).await?;
    debug!("RDY {}", rdy);
    Ok(())
}

pub(crate) async fn publish<State>(
    addr: String,
    state: State,
    cafile: Option<PathBuf>,
    auth: Option<String>,
    config: ConfigResponse,
    stream: &mut TcpStream,
    future: impl Publisher<State>,
) -> Result<Msg, ClientError> {
    let addr: Vec<&str> = addr.split(':').collect();
    let connector = if let Some(cafile) = cafile {
        connector_from_cafile(&cafile)
            .await
            .expect("failed to read cafile")
    } else {
        TlsConnector::new()
    };
    let mut tls_stream = connector.connect(addr[0], stream).unwrap().await?;
    let mut stream = conn::NsqStream::new(&mut tls_stream, 1024);
    stream.next().await.unwrap()?;
    debug!("TLS Ok");
    if config.auth_required {
        auth::authenticate(auth, &mut stream).await?;
    }
    let msg = future.call(state).await;
    stream.reset();
    conn::publish(&mut stream, msg).await
}

async fn connector_from_cafile(cafile: &Path) -> io::Result<TlsConnector> {
    let mut config = ClientConfig::new();
    let file = async_std::fs::read(cafile).await?;
    let mut pem = Cursor::new(file);
    config
        .root_store
        .add_pem_file(&mut pem)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid cert"))?;
    Ok(TlsConnector::from(Arc::new(config)))
}
