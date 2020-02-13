use crate::codec::{Message, Rdy};
use crate::error::ClientError;
use async_std::prelude::*;
use futures::AsyncWrite;

pub async fn rdy<IO: AsyncWrite + Unpin>(io: &mut IO, rdy: u32) -> Result<(), ClientError> {
    let buf: Message = Rdy::new(&rdy.to_string()).into();

    if let Err(e) = io.write_all(&buf[..]).await {
        return Err(ClientError::from(e));
    }

    Ok(())
}
