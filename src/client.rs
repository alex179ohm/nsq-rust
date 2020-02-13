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

use crate::config::{Config, ConfigResponse};
use crate::error::ClientError;
use crate::handler::Consumer;
use crate::handler::Publisher;
use crate::conn;
use crate::msg::Msg;
use async_std::net::{TcpStream, ToSocketAddrs};
use log::{debug, info};
use std::fmt::{Debug, Display};
use std::path::PathBuf;

#[derive(Clone)]
pub struct Client<State> {
    addr: String,
    config: Config,
    auth: Option<String>,
    cafile: Option<PathBuf>,
    rdy: u32,
    state: State,
}

impl Client<()> {
    pub fn new<ADDR: Into<String>>(
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
            rdy: 0,
            state: (),
        }
    }
}

impl<State> Client<State> {
    pub fn with_state<ADDR: Into<String>>(
        addr: ADDR,
        config: Config,
        auth: Option<String>,
        cafile: Option<PathBuf>,
        state: State,
    ) -> Self {
        Client {
            addr: addr.into(),
            config,
            auth,
            cafile,
            rdy: 0,
            state,
        }
    }
}

impl<State> Client<State> {
    pub async fn consumer<CHANNEL, TOPIC>(
        self,
        channel: CHANNEL,
        topic: TOPIC,
        _future: impl Consumer<State>,
    ) -> Result<(), ClientError>
    where
        CHANNEL: Into<String> + Display + Copy,
        TOPIC: Into<String> + Display + Copy,
    {
        let mut tcp_stream = connect(self.addr.clone()).await?;

        if let Err(e) = conn::magic(&mut tcp_stream).await {
            return Err(e);
        }

        let mut stream = conn::NsqStream::new(&mut tcp_stream, 1024);

        let resp = match conn::identify(&mut stream, self.config.clone()).await {
            Ok(Msg::Json(s)) => s,
            Ok(r) => {
                return Err(ClientError::from(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("unexpected response: {:?}", r),
                )))
            }
            Err(e) => return Err(e),
        };

        let nsqd_cfg: ConfigResponse = serde_json::from_str(&resp)?;
        debug!("{:?}", nsqd_cfg);
        debug!("Configuration OK: {:?}", nsqd_cfg);

        if nsqd_cfg.tls_v1 {
            conn::tls::consume(
                self.addr,
                self.auth,
                nsqd_cfg,
                self.cafile,
                &mut tcp_stream,
                channel,
                topic,
                self.rdy,
                _future,
            )
            .await
        } else {
            conn::tcp::consume(
                self.auth,
                nsqd_cfg,
                &mut stream,
                channel,
                topic,
                self.rdy,
                self.state,
                _future,
            )
            .await
        }
    }

    pub async fn publish(self, future: impl Publisher<State>) -> Result<Msg, ClientError> {

        let mut tcp_stream = connect(self.addr.clone()).await?;
        let mut stream = conn::NsqStream::new(&mut tcp_stream, 1024);

        if let Err(e) = conn::magic(&mut stream).await {
            return Err(e);
        }

        let nsqd_cfg: ConfigResponse;
        match conn::identify(&mut stream, self.config.clone()).await {
            Ok(Msg::Json(s)) => {
                nsqd_cfg = serde_json::from_str(&s).expect("json deserialize error")
            }
            Err(e) => return Err(e),
            _ => unreachable!(),
        }
        debug!("{:?}", nsqd_cfg);
        info!("Configuration OK: {:?}", nsqd_cfg);

        if nsqd_cfg.tls_v1 {
            conn::tls::publish(
                self.addr,
                self.state,
                self.cafile,
                self.auth,
                nsqd_cfg,
                &mut tcp_stream,
                future,
            )
            .await
        } else {
            conn::tcp::publish(self.auth, nsqd_cfg, &mut stream, self.state, future).await
        }
    }
}

#[doc(hidden)]
async fn connect<ADDR: ToSocketAddrs + Debug>(addr: ADDR) -> Result<TcpStream, ClientError> {
    debug!("Trying to connect to: {:?}", addr);
    match TcpStream::connect(addr).await {
        Ok(s) => {
            debug!("Connected: {:?}", s.peer_addr());
            Ok(s)
        }
        Err(e) => Err(ClientError::from(e)),
    }
}
