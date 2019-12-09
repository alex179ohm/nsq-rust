use crate::codec::decode_msg;
use crate::error::NsqError;
use crate::response::Response;
use crate::result::NsqResult;
use futures::io::{AsyncRead, AsyncWrite, Result};
use async_std::stream::Stream;
use byteorder::{BigEndian, ByteOrder};
use bytes::BytesMut;
use std::{
    convert::TryFrom,
    io as stdio,
    pin::Pin,
    str::from_utf8,
    task::{Context, Poll},
};
use log::debug;

const HEADER_SIZE: usize = 8;

pub struct NsqStream<'a, S: AsyncRead + AsyncWrite + Unpin> {
    stream: &'a mut S,
    read_buffer: BytesMut,
    exit: bool,
}

impl<'a, S: AsyncRead + AsyncWrite + Unpin> NsqStream<'a, S> {
    pub fn new(stream: &'a mut S, max_size: usize) -> Self {
        Self {
            stream,
            read_buffer: BytesMut::with_capacity(max_size),
            exit: false,
        }
    }
}

impl<'a, S: AsyncRead + AsyncWrite + Unpin> Stream for NsqStream<'a, S> {
    type Item = NsqResult<Response>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let mut buffer = [0u8; 1024];
        match Pin::new(&mut this.stream).poll_read(cx, &mut buffer) {
            Poll::Pending => {
                if this.exit {
                    return Poll::Ready(None);
                }
                return Poll::Pending;
            }
            Poll::Ready(Ok(l)) if l == 0 => {
                this.exit = true;
                let e = stdio::Error::new(stdio::ErrorKind::UnexpectedEof, "Received 0 bytes");
                return Poll::Ready(Some(Err(e.into())));
            }
            Poll::Ready(Ok(l)) => {
                this.read_buffer.extend(&buffer[..l]);
                debug!("{:?}", this.read_buffer);
                if l < HEADER_SIZE {
                    return Poll::Pending;
                }
                let size = usize::try_from(BigEndian::read_u32(&buffer[..4]))
                    .expect("cannot convert u32 to usize");
                if l < size {
                    return Poll::Pending;
                }
                let frame = BigEndian::read_u32(&this.read_buffer.split_to(8)[4..]);
                match frame {
                    0 => {
                        this.exit = true;
                        return Poll::Ready(Some(Ok(from_utf8(&this.read_buffer.split_to(size+4)[..])
                            .expect("failed to encode utf8")
                            .into())));
                    }
                    1 => {
                        this.exit = true;
                        return Poll::Ready(Some(Err(NsqError::from(
                            from_utf8(&this.read_buffer.split_to(size+4)[..]).expect("failed to encode utf8"),
                        ))));
                    }
                    2 => {
                        return Poll::Ready(Some(
                            Ok(decode_msg(&mut this.read_buffer.split_to(size+4)[..]).into()),
                        ));
                    }
                    _ => unreachable!(),
                }
            }
            Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e.into()))),
        }
    }
}

impl<'a, S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for NsqStream<'a, S> {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize>> {
        let this = self.get_mut();
        Pin::new(&mut this.stream).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut this.stream).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut this.stream).poll_close(cx)
    }
}

impl<'a, S: AsyncRead + AsyncWrite + Unpin> AsyncRead for NsqStream<'a, S> {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        let this = self.get_mut();
        Pin::new(&mut this.stream).poll_read(cx, buf)
    }
}
