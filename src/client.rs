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

use crate::auth::Authentication;
use crate::codec::Encoder;
use crate::config::{Config, NsqConfig};
use crate::error::NsqError;
use crate::io::NsqStream as NsqIO;
use crate::publish::io_pub;
use crate::response::Response;
use crate::result::NsqResult;
use crate::utils;
use async_std::io;
use async_std::net::{TcpStream, ToSocketAddrs};
use async_std::stream::StreamExt;
use async_tls::TlsConnector;
use bytes::BytesMut;
use futures::io::{AsyncRead, AsyncWrite};
use log::{debug, info};
use rustls::ClientConfig;
use std::fmt::{Debug, Display};
use std::future::Future;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Clone)]
pub struct Client {
    addr: String,
    config: Config,
    auth: Option<String>,
    cafile: Option<PathBuf>,
}

impl Client {
    pub fn new<ADDR: Into<String> + Debug>(
        addr: ADDR,
        config: Config,
        auth: Option<String>,
        cafile: Option<PathBuf>,
    ) -> Self {
        Client {
            addr: addr.into(),
            config,
            auth,
            cafile,
        }
    }

    pub async fn consumer<CHANNEL, TOPIC, F, T>(
        self,
        channel: CHANNEL,
        topic: TOPIC,
        _future: F,
    ) -> NsqResult<()>
    where
        CHANNEL: Into<String> + Display + Copy,
        TOPIC: Into<String> + Display + Copy,
        F: Future<Output = T> + Send + Sync + 'static,
        T: Encoder,
    {
        let mut buf = BytesMut::new();
        let mut tcp_stream = connect(self.addr.clone()).await?;
        if let Err(e) = utils::magic(&mut tcp_stream, &mut buf).await {
            return Err(e);
        }
        let mut stream = NsqIO::new(&mut tcp_stream, 1024);
        let nsqd_cfg: NsqConfig;
        match utils::identify(&mut stream, self.config.clone(), &mut buf).await {
            Ok(Response::Json(s)) => {
                nsqd_cfg = serde_json::from_str(&s).expect("json deserialize error")
            }
            Err(e) => return Err(e),
            _ => unreachable!(),
        }
        debug!("{:?}", nsqd_cfg);
        info!("Configuration OK: {:?}", nsqd_cfg);
        if nsqd_cfg.tls_v1 {
            self.consumer_tls(nsqd_cfg, &mut tcp_stream, buf, channel, topic, _future)
                .await
        } else {
            self.consumer_tcp(nsqd_cfg, &mut stream, buf, channel, topic, _future)
                .await
        }
    }

    async fn consumer_tls<CHANNEL, TOPIC, F, T>(
        self,
        config: NsqConfig,
        stream: &mut TcpStream,
        mut buf: BytesMut,
        channel: CHANNEL,
        topic: TOPIC,
        _future: F,
    ) -> NsqResult<()>
    where
        CHANNEL: Into<String> + Copy + Display,
        TOPIC: Into<String> + Copy + Display,
        F: Future<Output = T>,
        T: Encoder,
    {
        let addr: Vec<&str> = self.addr.split(':').collect();
        let cafile = self.cafile;
        let connector = if let Some(cafile) = cafile {
            connector_from_cafile(&cafile)
                .await
                .expect("failed to read cafile")
        } else {
            TlsConnector::new()
        };
        let mut tls_stream = connector.connect(addr[0], stream).unwrap().await?;
        let mut stream = NsqIO::new(&mut tls_stream, 1024);
        stream.next().await.unwrap()?;
        info!("TLS Ok");
        if config.auth_required && self.auth.is_some() {
            let auth_token = self.auth.unwrap();
            stream.reset();
            if let Response::Json(s) = utils::auth(&mut stream, auth_token, &mut buf).await? {
                let auth: Authentication =
                    serde_json::from_str(&s).expect("json deserialize error");
                info!("AUTH: {:?}", auth);
            }
        }
        let res = utils::sub(&mut stream, &mut buf, channel, topic).await?;
        info!("SUB: {} {}: {:?}", channel, topic, res);
        let _ = utils::rdy(&mut stream, &mut buf, 1).await?;
        info!("RDY 1");
        Ok(())
    }

    async fn consumer_tcp<CHANNEL, TOPIC, S, F, T>(
        self,
        config: NsqConfig,
        stream: &mut NsqIO<'_, S>,
        mut buf: BytesMut,
        channel: CHANNEL,
        topic: TOPIC,
        _future: F,
    ) -> NsqResult<()>
    where
        CHANNEL: Into<String> + Display + Copy,
        TOPIC: Into<String> + Display + Copy,
        S: AsyncRead + AsyncWrite + Unpin,
        F: Future<Output = T>,
        T: Encoder,
    {
        if config.auth_required && self.auth.is_some() {
            let auth_token = self.auth.unwrap();
            stream.reset();
            if let Response::Json(s) = utils::auth(stream, auth_token, &mut buf).await? {
                let auth: Authentication =
                    serde_json::from_str(&s).expect("json deserialize error");
                info!("AUTH: {:?}", auth);
            }
        }
        let res = utils::sub(stream, &mut buf, channel, topic).await?;
        info!("SUB {} {}: {:?}", channel, topic, res);
        let _ = utils::rdy(stream, &mut buf, 1).await?;
        info!("RDY 1");
        Ok(())
    }

    pub async fn publish<F, T>(self, future: F) -> NsqResult<Response>
    where
        F: Future<Output = T>,
        T: Encoder,
    {
        let mut buf = BytesMut::new();
        let mut tcp_stream = connect(self.addr.clone()).await?;
        let mut stream = NsqIO::new(&mut tcp_stream, 1024);
        if let Err(e) = utils::magic(&mut stream, &mut buf).await {
            return Err(e);
        }
        let nsqd_cfg: NsqConfig;
        match utils::identify(&mut stream, self.config.clone(), &mut buf).await {
            Ok(Response::Json(s)) => {
                nsqd_cfg = serde_json::from_str(&s).expect("json deserialize error")
            }
            Err(e) => return Err(e),
            _ => unreachable!(),
        }
        debug!("{:?}", nsqd_cfg);
        println!("Configuration OK: {:?}", nsqd_cfg);
        if nsqd_cfg.tls_v1 {
            self.publish_tls(nsqd_cfg, &mut tcp_stream, buf, future)
                .await
        } else {
            self.publish_tcp(nsqd_cfg, &mut stream, buf, future).await
        }
    }

    async fn publish_tls<F, T>(
        self,
        config: NsqConfig,
        stream: &mut TcpStream,
        mut buf: BytesMut,
        future: F,
    ) -> NsqResult<Response>
    where
        F: Future<Output = T>,
        T: Encoder,
    {
        let addr: Vec<&str> = self.addr.split(':').collect();
        let cafile = self.cafile;
        let connector = if let Some(cafile) = cafile {
            connector_from_cafile(&cafile)
                .await
                .expect("failed to read cafile")
        } else {
            TlsConnector::new()
        };
        let mut tls_stream = connector.connect(addr[0], stream).unwrap().await?;
        let mut stream = NsqIO::new(&mut tls_stream, 1024);
        stream.next().await.unwrap()?;
        info!("TLS Ok");
        if config.auth_required && self.auth.is_some() {
            let auth_token = self.auth.unwrap();
            stream.reset();
            if let Response::Json(s) = utils::auth(&mut stream, auth_token, &mut buf).await? {
                let auth: Authentication =
                    serde_json::from_str(&s).expect("json deserialize error");
                info!("AUTH: {:?}", auth);
            }
        }
        let msg = future.await;
        msg.encode(&mut buf);
        stream.reset();
        io_pub(&mut stream, &mut buf).await
    }

    async fn publish_tcp<F, T, S>(
        self,
        config: NsqConfig,
        stream: &mut NsqIO<'_, S>,
        mut buf: BytesMut,
        future: F,
    ) -> NsqResult<Response>
    where
        F: Future<Output = T>,
        T: Encoder,
        S: AsyncWrite + AsyncRead + Unpin,
    {
        if config.auth_required && self.auth.is_some() {
            let auth_token = self.auth.unwrap();
            stream.reset();
            if let Response::Json(s) = utils::auth(stream, auth_token, &mut buf).await? {
                let auth: Authentication =
                    serde_json::from_str(&s).expect("json deserialize error");
                info!("AUTH: {:?}", auth);
            }
        }
        let msg = future.await;
        msg.encode(&mut buf);
        stream.reset();
        io_pub(stream, &mut buf).await
    }
}

async fn connect<ADDR: ToSocketAddrs + Debug>(addr: ADDR) -> NsqResult<TcpStream> {
    debug!("Trying to connect to: {:?}", addr);
    match TcpStream::connect(addr).await {
        Ok(s) => {
            debug!("Connected: {:?}", s.peer_addr());
            Ok(s)
        }
        Err(e) => Err(NsqError::from(e)),
    }
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
