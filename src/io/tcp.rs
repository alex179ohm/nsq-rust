use crate::auth;
use crate::config::NsqConfig;
use crate::handler::Consumer;
use crate::handler::Publisher;
use crate::io::NsqIO;
use crate::msg::Msg;
use crate::error::NsqError;
use crate::utils;
use futures::{AsyncRead, AsyncWrite};
use log::debug;
use std::fmt::Display;

#[allow(clippy::too_many_arguments)]
pub(crate) async fn consumer<CHANNEL, TOPIC, S, State>(
    auth: Option<String>,
    config: NsqConfig,
    stream: &mut NsqIO<'_, S>,
    channel: CHANNEL,
    topic: TOPIC,
    rdy: u32,
    _state: State,
    _future: impl Consumer<State>,
) -> Result<(), NsqError>
where
    CHANNEL: Into<String> + Display + Copy,
    TOPIC: Into<String> + Display + Copy,
    S: AsyncRead + AsyncWrite + Unpin,
{
    if config.auth_required {
        auth::authenticate(auth, stream).await?;
    }
    let res = utils::sub(stream, channel, topic).await?;
    debug!("SUB {} {}: {:?}", channel, topic, res);
    utils::rdy(stream, rdy).await?;
    debug!("RDY {}", rdy);
    Ok(())
}

pub(crate) async fn publish<S, State>(
    auth: Option<String>,
    config: NsqConfig,
    stream: &mut NsqIO<'_, S>,
    state: State,
    future: impl Publisher<State>,
) -> Result<Msg, NsqError>
where
    S: AsyncWrite + AsyncRead + Unpin,
{
    if config.auth_required {
        auth::authenticate(auth, stream).await?;
    }
    let msg = future.call(state).await;
    stream.reset();
    utils::io_publish(stream, msg).await
}
