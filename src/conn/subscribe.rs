use crate::codec::{Message, Sub};
use crate::error::ClientError;
use crate::msg::Msg;
use async_std::prelude::*;
use futures::{AsyncWrite, Stream};

pub(crate) async fn subscribe<IO, CHANNEL, TOPIC>(
    io: &mut IO,
    channel: CHANNEL,
    topic: TOPIC,
) -> Result<Msg, ClientError>
where
    IO: AsyncWrite + Stream<Item = Result<Msg, ClientError>> + Unpin,
    CHANNEL: Into<String>,
    TOPIC: Into<String>,
{
    let buf: Message = Sub::new(&channel.into(), &topic.into()).into();

    if let Err(e) = io.write_all(&buf[..]).await {
        return Err(ClientError::from(e));
    }

    io.next().await.unwrap()
}
