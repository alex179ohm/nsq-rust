use crate::config::Config;
use crate::response::Response;
use async_std::io::prelude::*;
use async_std::stream::StreamExt;
use futures::Stream;
use crate::codec::{Magic, Identify, Auth, Encoder};
use bytes::BytesMut;
use crate::result::NsqResult;
use crate::error::NsqError;

pub(crate) async fn magic<IO: Write + Unpin>(io: &mut IO, buf: &mut BytesMut) -> NsqResult<()> {
    Magic{}.encode(buf);
    if let Err(e) = io.write_all(&buf.take()[..]).await {
        return Err(NsqError::from(e))
    };
    Ok(())
}

pub(crate) async  fn identify<IO: Write + Stream<Item = NsqResult<Response>> + Unpin>(
    io: &mut IO,
    config: Config,
    buf: &mut BytesMut,
) -> NsqResult<Response> {
    if let Ok(msg_string) = serde_json::to_string(&config) {
        Identify::new(msg_string.as_str()).encode(buf);
    };
    if let Err(e) = io.write_all(&buf.take()[..]).await {
        return Err(NsqError::from(e));
    };
    io.next().await.unwrap()
}

pub(crate) async fn auth<IO, AUTH>(io: &mut IO, auth: AUTH, buf: &mut BytesMut) -> NsqResult<Response>
where
    IO: Write + Stream<Item = NsqResult<Response>> + Unpin,
    AUTH: Into<String>,
{
    Auth::new(auth.into().as_str()).encode(buf);
    if let Err(e) = io.write_all(&buf.take()[..]).await {
        return Err(NsqError::from(e));
    };
    io.next().await.unwrap()
}

//pub(crate) async fn cls<IO>(io: &mut IO, buf: &mut BytesMut) -> NsqResult<Response>
//where
//    IO: Write + Stream<Item = NsqResult<Response>> + Unpin,
//{
//    Cls{}.encode(buf);
//    if let Err(e) = io.write_all(&buf.take()[..]).await {
//        return Err(NsqError::from(e));
//    }
//    io.next().await.unwrap()
//}

//pub (crate) async fn rdy<IO: Write + Unpin>(io: &mut IO, n: &str, buf: &mut BytesMut) -> NsqResult<()> {
//    Rdy::new(n).encode(buf);
//    if let Err(e) =  io.write_all(&buf.take()[..]).await {
//        return Err(NsqError::from(e));
//    };
//    Ok(())
//}
