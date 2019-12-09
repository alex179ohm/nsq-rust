use futures::io::{AsyncWrite, AsyncWriteExt};
use futures::Stream;
use async_std::stream::StreamExt;
use bytes::BytesMut;
use crate::result::NsqResult;
use crate::response::Response;
use crate::error::NsqError;

pub async fn io_pub<S>(io: &mut S, buf: &mut BytesMut) -> NsqResult<Response>
    where
        S: AsyncWrite + Stream<Item = NsqResult<Response>> + Unpin,
{
    if let Err(e) = io.write_all(&buf.take()[..]).await {
        return Err(NsqError::from(e));
    }
    io.next().await.unwrap()
}
