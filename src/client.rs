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
use crate::io::NsqStream;
use crate::response::Response;
use crate::config::{Config, NsqConfig};
use bytes::BytesMut;
use std::future::Future;
use async_tls::TlsConnector;
use rustls::ClientConfig;
use std::path::{Path, PathBuf};
use crate::publish::io_pub;

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

    pub async fn start(self, buf: &mut BytesMut) -> NsqResult<()> {
        let mut tcp_stream = connect(self.addr.clone()).await?;
        if let Err(e) = utils::magic(&mut tcp_stream, buf).await {
            return Err(e);
        }
        let mut stream = NsqStream::new(&mut tcp_stream, 1024);
        let nsqd_cfg: NsqConfig;
        match utils::identify(&mut stream, self.config.clone(), buf).await {
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
        let mut tls_stream = connector.connect(addr[0], stream).unwrap().await?;
        let mut stream = NsqStream::new(&mut tls_stream, 1024);
        if let Err(e) = stream.next().await.unwrap() {
            debug!("TLS connection error: {:?}", e);
            return Err(e);
        }
        info!("TLS Ok");
        Ok(())
    }

    pub async fn publish<F, T>(self, future: F) -> NsqResult<Response>
    where
        F: Future<Output = T>,
        T: Encoder,
    {
        let mut buf = BytesMut::new();
        let mut tcp_stream = connect(self.addr.clone()).await?;
        let mut stream = NsqStream::new(&mut tcp_stream, 1024);
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
        let mut tls_stream = connector.connect(addr[0], tcp_stream).unwrap().await?;
        let mut stream = NsqStream::new(&mut tls_stream, 1024);
        let res = match stream.next().await.unwrap() {
            Err(e) => {
                println!("TLS connection error: {:?}", e);
                return Err(e);
            },
            Ok(s) => {
                s
            },
        };
        info!("TLS: {:?}", res);
        if nsqd_cfg.auth_required && self.auth.is_some() {
            let auth_token = self.auth.unwrap();
            let mut stream = NsqStream::new(&mut tls_stream, 1024);
            if let Response::Json(s) = utils::auth(&mut stream, auth_token, &mut buf).await? {
                let auth: Authentication = serde_json::from_str(&s).expect("json deserialize error");
                info!("AUTH: {:?}", auth);
            }
        }
        let msg = future.await;
        msg.encode(&mut buf);
        let mut stream = NsqStream::new(&mut tls_stream, 1024);
        io_pub(&mut stream, &mut buf).await
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
