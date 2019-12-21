use crate::auth;
use crate::config::NsqConfig;
use crate::consumer::Consumer;
use crate::publisher::Publisher;
use crate::io::NsqIO;
use crate::result::NsqResult;
use crate::utils;
use crate::msg::Msg;
use futures::{AsyncRead, AsyncWrite};
use log::debug;
use std::fmt::Display;

pub(crate) async fn consumer<CHANNEL, TOPIC, S, State>(
    auth: Option<String>,
    config: NsqConfig,
    stream: &mut NsqIO<'_, S>,
    channel: CHANNEL,
    topic: TOPIC,
    rdy: u32,
    _state: State,
    _future: impl Consumer<State>,
) -> NsqResult<()>
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
) -> NsqResult<Msg>
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
