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
use crate::config::{Config, NsqConfig};
use crate::consumer::Consumer;
use crate::error::NsqError;
use crate::io::NsqIO;
use crate::msg::Msg;
use crate::publisher::Publisher;
use crate::result::NsqResult;
use crate::tls;
use crate::utils;
use async_std::net::{TcpStream, ToSocketAddrs};
use futures::io::{AsyncRead, AsyncWrite};
use log::debug;
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
    ) -> NsqResult<()>
    where
        CHANNEL: Into<String> + Display + Copy,
        TOPIC: Into<String> + Display + Copy,
    {
        let mut tcp_stream = connect(self.addr.clone()).await?;
        if let Err(e) = utils::magic(&mut tcp_stream).await {
            return Err(e);
        }
        let mut stream = NsqIO::new(&mut tcp_stream, 1024);
        let nsqd_cfg: NsqConfig;
        if let Msg::Json(s) = utils::identify(&mut stream, self.config.clone()).await? {
            //            Ok(Msg::Json(s)) => {
            nsqd_cfg = serde_json::from_str(&s)?;
            //            }
            //            Err(e) => return Err(e),
            //            _ => unreachable!(),
            debug!("{:?}", nsqd_cfg);
            debug!("Configuration OK: {:?}", nsqd_cfg);
        } else {
            unreachable!()
        };
        if nsqd_cfg.tls_v1 {
            tls::consumer_tls(
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
            self.consumer_tcp(nsqd_cfg, &mut stream, channel, topic, _future)
                .await
        }
    }

    async fn consumer_tcp<CHANNEL, TOPIC, S>(
        self,
        config: NsqConfig,
        stream: &mut NsqIO<'_, S>,
        channel: CHANNEL,
        topic: TOPIC,
        _future: impl Consumer<State>,
    ) -> NsqResult<()>
    where
        CHANNEL: Into<String> + Display + Copy,
        TOPIC: Into<String> + Display + Copy,
        S: AsyncRead + AsyncWrite + Unpin,
    {
        if config.auth_required && self.auth.is_some() {
            let auth_token = self.auth.unwrap();
            stream.reset();
            if let Msg::Json(s) = utils::auth(stream, auth_token).await? {
                let auth: auth::Reply = serde_json::from_str(&s).expect("json deserialize error");
                debug!("AUTH: {:?}", auth);
            }
        }
        let res = utils::sub(stream, channel, topic).await?;
        debug!("SUB {} {}: {:?}", channel, topic, res);
        utils::rdy(stream, self.rdy).await?;
        debug!("RDY {}", self.rdy);
        Ok(())
    }

    pub async fn publish(self, future: impl Publisher<State>) -> NsqResult<Msg> {
        let mut tcp_stream = connect(self.addr.clone()).await?;
        let mut stream = NsqIO::new(&mut tcp_stream, 1024);
        if let Err(e) = utils::magic(&mut stream).await {
            return Err(e);
        }
        let nsqd_cfg: NsqConfig;
        match utils::identify(&mut stream, self.config.clone()).await {
            Ok(Msg::Json(s)) => {
                nsqd_cfg = serde_json::from_str(&s).expect("json deserialize error")
            }
            Err(e) => return Err(e),
            _ => unreachable!(),
        }
        debug!("{:?}", nsqd_cfg);
        println!("Configuration OK: {:?}", nsqd_cfg);
        if nsqd_cfg.tls_v1 {
            tls::publish_tls(
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
            self.publish_tcp(nsqd_cfg, &mut stream, future).await
        }
    }

    async fn publish_tcp<S>(
        self,
        config: NsqConfig,
        stream: &mut NsqIO<'_, S>,
        future: impl Publisher<State>,
    ) -> NsqResult<Msg>
    where
        S: AsyncWrite + AsyncRead + Unpin,
    {
        if config.auth_required && self.auth.is_some() {
            let auth_token = self.auth.unwrap();
            stream.reset();
            if let Msg::Json(s) = utils::auth(stream, auth_token).await? {
                let auth: auth::Reply = serde_json::from_str(&s).expect("json deserialize error");
                debug!("AUTH: {:?}", auth);
            }
        }
        let msg = future.call(self.state).await;
        stream.reset();
        utils::io_publish(stream, msg).await
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
