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

use crate::config::Config;
use crate::conn;
use crate::handler::Handler;
use crate::publisher::Publisher;
use crate::response::Response;
use async_std::net::{TcpStream, ToSocketAddrs};
use log::{debug, info};
use std::fmt::{Debug, Display};
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct Client<State: Debug> {
    addr: String,
    config: Config,
    auth: Option<String>,
    cafile: Option<PathBuf>,
    rdy: u32,
    state: State,
}

pub struct Builder {
    addr: String,
    config: Config,
    auth: Option<String>,
    cafile: Option<PathBuf>,
    rdy: u32,
}

impl Default for Builder {
    fn default() -> Self {
        Builder {
            addr: String::new(),
            config: Config::default(),
            auth: None,
            cafile: None,
            rdy: 0,
        }
    }
}

impl Builder {
    #[must_use]
    pub fn new() -> Builder {
        Builder::default()
    }

    pub fn addr(mut self, addr: impl Into<String>) -> Builder {
        self.addr = addr.into();
        self
    }

    #[must_use]
    pub fn config(mut self, config: Config) -> Builder {
        self.config = config;
        self
    }

    pub fn auth(mut self, auth: impl Into<String>) -> Builder {
        self.auth = Some(auth.into());
        self
    }

    pub fn cafile(mut self, cafile: impl Into<PathBuf>) -> Builder {
        self.cafile = Some(cafile.into());
        self
    }

    #[must_use]
    pub fn rdy(mut self, rdy: u32) -> Builder {
        self.rdy = rdy;
        self
    }

    #[must_use]
    pub fn build(self) -> Client<()> {
        Client {
            addr: self.addr,
            config: self.config,
            auth: self.auth,
            cafile: self.cafile,
            rdy: self.rdy,
            state: (),
        }
    }

    pub fn build_with_state<State: Debug>(self, state: State) -> Client<State> {
        Client {
            addr: self.addr,
            config: self.config,
            auth: self.auth,
            cafile: self.cafile,
            rdy: self.rdy,
            state,
        }
    }
}

impl<State: Debug> Client<State> {
    pub async fn consumer<CHANNEL, TOPIC>(
        self,
        channel: CHANNEL,
        topic: TOPIC,
        _future: impl Handler<State>,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        CHANNEL: Into<String> + Display + Copy,
        TOPIC: Into<String> + Display + Copy,
    {
        let mut stream = connect(self.addr.clone()).await?;

        conn::magic(&mut stream).await?;

        //let mut stream = conn::NsqStream::new(&mut tcp_stream, 1024);

        let cfg = conn::identify(&mut stream, self.config.clone()).await?;

        debug!("{:?}", cfg);
        debug!("Configuration OK: {:?}", cfg);

        Ok(())
    }

    pub async fn connect<S: ToSocketAddrs>(self, _addr: S) -> Client<State> {
        self
    }

    pub async fn publish(
        self,
        future: impl Publisher<State>,
    ) -> Result<Response, Box<dyn std::error::Error>> {
        let mut stream = connect(self.addr.clone()).await?;

        conn::magic(&mut stream).await?;

        let cfg = conn::identify(&mut stream, self.config.clone()).await?;

        debug!("{:?}", cfg);
        info!("Configuration OK: {:?}", cfg);

        Ok(Response::Ok)
    }
}

#[doc(hidden)]
async fn connect<ADDR: ToSocketAddrs + Debug>(
    addr: ADDR,
) -> Result<TcpStream, Box<dyn std::error::Error>> {
    debug!("Trying to connect to: {:?}", addr);
    match TcpStream::connect(addr).await {
        Ok(s) => {
            debug!("Connected: {:?}", s.peer_addr());
            Ok(s)
        }
        Err(e) => Err(Box::new(e)),
    }
}
