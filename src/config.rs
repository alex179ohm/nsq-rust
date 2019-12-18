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

/// Configuration sent to nsqd to properly config the [Connection](struct.Connection.html)
///
/// # Examples
///```no-run
/// use nsq_client::{Client, Config};
///
/// let config = Config::new().client_id("consumer").user_agent("node-1");
/// Client::new(
///     "test",
///     "test",
///     "0.0.0.0:4150",
///     Some(config),
///     None,
///     None,
/// );
///```
///
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

    /// Size of the buffer (in bytes) used by nsqd for buffering writes to this connection
    ///
    /// Valid values:
    /// * -1 disable output buffer
    /// * 64 <= output_buffer_size <= configured_max
    ///
    /// Default: **16384**
    pub output_buffer_size: u64,

    /// The timeout after which data nsqd has buffered will be flushed to this client.
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
}
use hostname;

fn get_hostname() -> Option<String> {
    if let Ok(h) = hostname::get() {
        if let Ok(s) = h.into_string() {
            return Some(s);
        }
        return None;
    }
    None
}

impl Default for Config {
    fn default() -> Config {
        Config {
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
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Default)]
pub struct NsqConfig {
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

#[allow(dead_code)]
impl Config {
    /// Create default [Config](struct.Config.html)
    /// ```no-run
    /// use nsq_client::{Config};
    ///
    /// let config = Config::new();
    /// assert_eq!(config, Config::default());
    /// ```
    pub fn new() -> Config {
        Config {
            ..Default::default()
        }
    }

    /// Change [client_id](struct.Config.html#structfield.client_id)
    /// ```no-run
    /// use nsq_client::Config;
    ///
    /// let config = Config::new().client_id("consumer");
    /// assert_eq!(config.client_id, Some("consumer".to_owned()));
    /// ```
    pub fn client_id(mut self, client_id: &str) -> Self {
        self.client_id = Some(client_id.to_owned());
        self
    }

    /// Change [hostname](struct.Config.html#structfield.hostname)
    /// ```no-run
    /// use nsq_client::Config;
    ///
    /// let config = Config::new().hostname("node-1");
    /// assert_eq!(config.hostname, Some("node-1".to_owned()));
    /// ```
    pub fn hostname<H: Into<String>>(mut self, hostname: H) -> Self {
        self.hostname = Some(hostname.into());
        self
    }

    /// Change [user_agent](struct.Config.html#structfield.user_agent)
    /// ```no-run
    /// use nsq_client::Config;
    ///
    /// let config = Config::new().user_agent("consumer-1");
    /// assert_eq!(config.user_agent, Some("consumer-1".to_owned()));
    /// ```
    pub fn user_agent<UA: Into<String>>(mut self, user_agent: UA) -> Self {
        self.user_agent = user_agent.into();
        self
    }

    pub fn tls_v1(&mut self, tls: bool) {
        self.tls_v1 = tls;
    }
}
