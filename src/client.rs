// MIT License
//
// Copyright (c) 2019-2021 Alessandro Cresto Miseroglio <alex179ohm@gmail.com>
// Copyright (c) 2019-2021 Tangram Technologies S.R.L. <https://tngrm.io>
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

use async_std::net::{ToSocketAddrs, TcpStream};
use async_std::stream::StreamExt;
use async_std::io;
use log::{debug, info};
use std::fmt::Debug;
use std::io::Cursor;
use std::sync::Arc;
use crate::error::NsqError;
use crate::codec::Encoder;
use crate::utils;
use crate::result::NsqResult;
use crate::auth::Authentication;
use crate::io::NsqStream as NsqIO;
use crate::response::Response;
use crate::config::{Config, NsqConfig};
use bytes::BytesMut;
use std::future::Future;
use async_tls::TlsConnector;
use rustls::ClientConfig;
use std::path::{Path, PathBuf};
use crate::publish::io_pub;

#[derive(Clone)]
pub struct Client {
    addr: String,
    config: Config,
    auth: Option<String>,
    cafile: Option<PathBuf>,
}

impl Client {
    pub fn new<ADDR: Into<String> + Debug>(addr: ADDR, config: Config, auth: Option<String>, cafile: Option<PathBuf>) -> Self {
        Client {
            addr: addr.into(),
            config,
            auth,
            cafile,
        }
    }

    pub async fn consumer(self, buf: &mut BytesMut) -> NsqResult<()> {
        let mut tcp_stream = connect(self.addr.clone()).await?;
        if let Err(e) = utils::magic(&mut tcp_stream, buf).await {
            return Err(e);
        }
        let mut io = NsqIO::new(&mut tcp_stream, 1024);
        let nsqd_cfg: NsqConfig;
        match utils::identify(&mut io, self.config.clone(), buf).await {
            Ok(Response::Json(s)) => nsqd_cfg = serde_json::from_str(&s).expect("json deserialize error"),
            Err(e) => return Err(e),
            _ => unreachable!(),
        }
        debug!("{:?}", nsqd_cfg);
        info!("Configuration OK: {:?}", nsqd_cfg);
        let addr: Vec<&str> = self.addr.split(':').collect();
        let cafile = self.cafile;
        let connector = if let Some(cafile) = cafile {
            connector_from_cafile(&cafile).await.expect("failed to read cafile")
        } else {
            TlsConnector::new()
        };
        if nsqd_cfg.tls_v1 {
            let mut tls_stream = connector.connect(addr[0], tcp_stream).unwrap().await?;
            let mut stream = NsqIO::new(&mut tls_stream, 1024);
            stream.next().await.unwrap()?;
            info!("TLS Ok");
            Ok(())
        } else {
            Ok(())
        }
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
            Ok(Response::Json(s)) => nsqd_cfg = serde_json::from_str(&s).expect("json deserialize error"),
            Err(e) => return Err(e),
            _ => unreachable!(),
        }
        debug!("{:?}", nsqd_cfg);
        println!("Configuration OK: {:?}", nsqd_cfg);
        let addr: Vec<&str> = self.addr.split(':').collect();
        let cafile = self.cafile;
        let connector = if let Some(cafile) = cafile {
            connector_from_cafile(&cafile).await.expect("failed to read cafile")
        } else {
            TlsConnector::new()
        };
        if nsqd_cfg.tls_v1 {
            let mut tls_stream = connector.connect(addr[0], tcp_stream).unwrap().await?;
            let mut stream = NsqIO::new(&mut tls_stream, 1024);
            stream.next().await.unwrap()?;
            info!("TLS Ok");
            if nsqd_cfg.auth_required && self.auth.is_some() {
                let auth_token = self.auth.unwrap();
                stream.reset();
                if let Response::Json(s) = utils::auth(&mut stream, auth_token, &mut buf).await? {
                    let auth: Authentication = serde_json::from_str(&s).expect("json deserialize error");
                    info!("AUTH: {:?}", auth);
                }
            }
            let msg = future.await;
            msg.encode(&mut buf);
            stream.reset();
            io_pub(&mut stream, &mut buf).await
        } else {
            if nsqd_cfg.auth_required && self.auth.is_some() {
                let auth_token = self.auth.unwrap();
                stream.reset();
                if let Response::Json(s) = utils::auth(&mut stream, auth_token, &mut buf).await? {
                    let auth: Authentication = serde_json::from_str(&s).expect("json deserialize error");
                    info!("AUTH: {:?}", auth);
                }
            }
            let msg = future.await;
            msg.encode(&mut buf);
            stream.reset();
            io_pub(&mut stream, &mut buf).await
        }
    }
}

async fn connect<ADDR: ToSocketAddrs + Debug>(addr: ADDR) -> NsqResult<TcpStream> {
    debug!("Trying to connect to: {:?}", addr);
    match TcpStream::connect(addr).await {
        Ok(s) => {
            debug!("Connected: {:?}", s.peer_addr());
            Ok(s)
        },
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
