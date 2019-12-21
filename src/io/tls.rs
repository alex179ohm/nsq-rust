use crate::auth;
use crate::config::NsqConfig;
use crate::handler::Consumer;
use crate::io::NsqIO;
use crate::msg::Msg;
use crate::handler::Publisher;
use crate::result::NsqResult;
use crate::utils;
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

pub(crate) async fn consumer<CHANNEL, TOPIC, State>(
    addr: String,
    auth: Option<String>,
    config: NsqConfig,
    cafile: Option<PathBuf>,
    stream: &mut TcpStream,
    channel: CHANNEL,
    topic: TOPIC,
    rdy: u32,
    _future: impl Consumer<State>,
) -> NsqResult<()>
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
    let mut stream = NsqIO::new(&mut tls_stream, 1024);
    stream.next().await.unwrap()?;
    debug!("TLS Ok");
    if config.auth_required {
        auth::authenticate(auth, &mut stream).await?;
    }
    let res = utils::sub(&mut stream, channel, topic).await?;
    debug!("SUB: {} {}: {:?}", channel, topic, res);
    utils::rdy(&mut stream, rdy).await?;
    debug!("RDY {}", rdy);
    Ok(())
}

pub(crate) async fn publish<State>(
    addr: String,
    state: State,
    cafile: Option<PathBuf>,
    auth: Option<String>,
    config: NsqConfig,
    stream: &mut TcpStream,
    future: impl Publisher<State>,
) -> NsqResult<Msg> {
    let addr: Vec<&str> = addr.split(':').collect();
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
    debug!("TLS Ok");
    if config.auth_required {
        auth::authenticate(auth, &mut stream).await?;
    }
    let msg = future.call(state).await;
    stream.reset();
    utils::io_publish(&mut stream, msg).await
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
