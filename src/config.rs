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

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Configuration sent to nsqd to properly config the [Client](struct.Client.html) -> nsqd
/// Connection.
///
/// # Examples
///```
/// //let config = Config::new().client_id("consumer").user_agent("node-1");
/// //println!("{:?}", config);
///```
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Config {
    /// Identifiers sent to nsqd representing this client (consumer specific)
    ///
    /// Default: **hostname** where connection is started
    pub client_id: Option<String>,

    /// Hostname where client is deployed.
    ///
    /// Default: **hostname** where connection is started
    pub hostname: Option<String>,

    /// Enable feature_negotiation
    ///
    /// Default: **true**
    pub feature_negotiation: bool,

    /// Duration of time between heartbeats (milliseconds).
    ///
    /// Valid values:
    /// * -1 disables heartbeats
    /// * 1000 <= heartbeat_interval <= configured_max
    ///
    /// Default: **30000**
    pub heartbeat_interval: i64,

    /// Size of the buffer (in bytes) used by nsqd for buffering writes to this
    /// connection
    ///
    /// Valid values:
    /// * -1 disable output buffer
    /// * 64 <= output_buffer_size <= configured_max
    ///
    /// Default: **16384**
    pub output_buffer_size: u64,

    /// The timeout after which data nsqd has buffered will be flushed to this
    /// client.
    ///
    /// Valid values:
    /// * -1 disable buffer timeout
    /// * 1ms <= output_buffer_timeout <= configured_max
    ///
    /// Default: **250**
    pub output_buffer_timeout: u32,

    /// Enable TLS negotiation
    ///
    /// Default: **false** (Not implemented)
    tls_v1: bool,

    /// Enable snappy compression.
    ///
    /// Default: **false** (Not implemented)
    pub snappy: bool,

    /// Enable deflate compression.
    ///
    /// Default: **false** (Not implemented)
    deflate: bool,
    /// Configure deflate compression level.
    ///
    /// Valid range:
    /// * 1 <= deflate_level <= configured_max
    ///
    /// Default: **6**
    deflate_level: u16,

    /// Integer percentage to sample the channel.
    ///
    /// Deliver a perventage of all messages received to this connection.
    ///
    /// Default: **0**
    pub sample_rate: u16,

    /// String indentifying the agent for this connection.
    ///
    /// Default: **hostname** where connection is started
    pub user_agent: String,

    /// Timeout used by nsqd before flushing buffered writes (set to 0 to disable).
    ///
    /// Default: **0**
    pub message_timeout: u32,

    #[serde(skip)]
    cafile: Option<PathBuf>,

    #[serde(skip)]
    auth: Option<String>,
}

fn get_hostname() -> Option<String> {
    if let Ok(h) = hostname::get() {
        if let Ok(s) = h.into_string() {
            return Some(s);
        }
        return None;
    }
    None
}

#[derive(Clone, Debug, Deserialize, PartialEq, Default)]
pub struct ConfigResponse {
    pub max_rdy_count: u32,
    pub version: String,
    pub max_msg_timeout: u64,
    pub msg_timeout: u64,
    pub tls_v1: bool,
    pub deflate: bool,
    pub deflate_level: u16,
    pub max_deflate_level: u16,
    pub snappy: bool,
    pub sample_rate: u16,
    pub auth_required: bool,
    pub output_buffer_size: u64,
    pub output_buffer_timeout: u32,
}

pub struct ConfigBuilder {
    client_id: Option<String>,
    user_agent: String,
    hostname: Option<String>,
    deflate: bool,
    deflate_level: u16,
    snappy: bool,
    tls_v1: bool,
    feature_negotiation: bool,
    heartbeat_interval: i64,
    message_timeout: u32,
    output_buffer_size: u64,
    output_buffer_timeout: u32,
    sample_rate: u16,
    cafile: Option<PathBuf>,
    auth: Option<String>,
}

impl Default for ConfigBuilder {
    fn default() -> Self {
        Self {
            client_id: get_hostname(),
            user_agent: String::from("rust nsq"),
            hostname: get_hostname(),
            deflate: false,
            deflate_level: 6,
            snappy: false,
            tls_v1: true,
            feature_negotiation: true,
            heartbeat_interval: 30000,
            message_timeout: 0,
            output_buffer_size: 16384,
            output_buffer_timeout: 250,
            sample_rate: 0,
            cafile: None,
            auth: None,
        }
    }
}

impl ConfigBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn client_id(mut self, id: Option<String>) -> ConfigBuilder {
        self.client_id = id;
        self
    }

    pub fn user_agent(mut self, ua: String) -> ConfigBuilder {
        self.user_agent = ua;
        self
    }

    pub fn hostname(mut self, hostname: Option<String>) -> ConfigBuilder {
        self.hostname = hostname;
        self
    }

    pub fn deflate(mut self, d: bool) -> ConfigBuilder {
        self.deflate = d;
        self
    }

    pub fn deflate_level(mut self, dl: u16) -> ConfigBuilder {
        self.deflate_level = dl;
        self
    }

    pub fn snappy(mut self, s: bool) -> ConfigBuilder {
        self.snappy = s;
        self
    }

    pub fn tls_v1(mut self, tls: bool) -> ConfigBuilder {
        self.tls_v1 = tls;
        self
    }

    pub fn feature_negotiation(mut self, negotiation: bool) -> ConfigBuilder {
        self.feature_negotiation = negotiation;
        self
   }

    pub fn heartbeat_interval(mut self, interval: i64) -> ConfigBuilder {
        self.heartbeat_interval = interval;
        self
    }

    pub fn message_timeout(mut self, timeout: u32) -> ConfigBuilder {
        self.message_timeout = timeout;
        self
    }

    pub fn output_buffer_size(mut self, size: u64) -> ConfigBuilder {
        self.output_buffer_size = size;
        self
    }

    pub fn output_buffer_timeout(mut self, timeout: u32) -> ConfigBuilder {
        self.output_buffer_timeout = timeout;
        self
    }

    pub fn sample_rate(mut self, rate: u16) -> ConfigBuilder {
        self.sample_rate = rate;
        self
    }

    /// Set the cafile for the client -> server tls handshake.
    pub fn cafile<Path: Into<PathBuf>>(mut self, cafile: Path) -> ConfigBuilder {
        self.cafile = Some(cafile.into());
        self
    }

    /// Set the auth token for nsqd -> client [authentication](fn.authenticate.html).
    pub fn auth<AUTH: Into<String>>(mut self, auth: AUTH) -> ConfigBuilder {
        self.auth = Some(auth.into());
        self
    }
}

impl From<ConfigBuilder> for Config {
    fn from(builder: ConfigBuilder) -> Config {
        Config {
            client_id: builder.client_id,
            hostname: builder.hostname,
            feature_negotiation: builder.feature_negotiation,
            heartbeat_interval: builder.heartbeat_interval,
            output_buffer_size: builder.output_buffer_size,
            output_buffer_timeout: builder.output_buffer_timeout,
            tls_v1: builder.tls_v1,
            snappy: builder.snappy,
            deflate: builder.deflate,
            deflate_level: builder.deflate_level,
            user_agent: builder.user_agent,
            sample_rate: builder.sample_rate,
            message_timeout: builder.message_timeout,
            cafile: builder.cafile,
            auth: builder.auth,
        }
    }
}
