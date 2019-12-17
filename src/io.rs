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
//ext install TabNine.tabnine-vscode
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

use crate::codec::decode_msg;
use crate::error::NsqError;
use crate::msg::Msg;
use crate::result::NsqResult;
use async_std::stream::Stream;
use byteorder::{BigEndian, ByteOrder};
use bytes::BytesMut;
use futures::io::{AsyncRead, AsyncWrite, Result};
use log::debug;
use std::{
    convert::TryFrom,
    io as stdio,
    pin::Pin,
    str::from_utf8,
    task::{Context, Poll},
};

const HEADER_SIZE: usize = 8;

pub struct NsqIO<'a, S> {
    stream: &'a mut S,
    read_buffer: BytesMut,
    exit: bool,
}

impl<'a, S> NsqIO<'a, S> {
    pub fn new(stream: &'a mut S, max_size: usize) -> Self {
        Self {
            stream,
            read_buffer: BytesMut::with_capacity(max_size),
            exit: false,
        }
    }

    pub fn reset(&mut self) {
        self.exit = false;
    }
}

impl<'a, S: AsyncRead + Unpin> Stream for NsqIO<'a, S> {
    type Item = NsqResult<Msg>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let mut buffer = [0u8; 1024];
        match Pin::new(&mut this.stream).poll_read(cx, &mut buffer) {
            Poll::Pending => {
                if this.exit {
                    Poll::Ready(None)
                } else {
                    Poll::Pending
                }
            }
            Poll::Ready(Ok(l)) if l == 0 => {
                this.exit = true;
                let e = stdio::Error::new(stdio::ErrorKind::UnexpectedEof, "Received 0 bytes");
                Poll::Ready(Some(Err(e.into())))
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
                        Poll::Ready(Some(Ok(from_utf8(
                            &this.read_buffer.split_to(size + 4)[..],
                        )
                        .expect("failed to encode utf8")
                        .into())))
                    }
                    1 => {
                        this.exit = true;
                        Poll::Ready(Some(Err(NsqError::from(
                            from_utf8(&this.read_buffer.split_to(size + 4)[..])
                                .expect("failed to encode utf8"),
                        ))))
                    }
                    2 => Poll::Ready(Some(Ok(decode_msg(
                        &mut this.read_buffer.split_to(size + 4)[..],
                    )
                    .into()))),
                    _ => unreachable!(),
                }
            }
            Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e.into()))),
        }
    }
}

impl<'a, S: AsyncWrite + Unpin> AsyncWrite for NsqIO<'a, S> {
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

impl<'a, S: AsyncRead + Unpin> AsyncRead for NsqIO<'a, S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        let this = self.get_mut();
        Pin::new(&mut this.stream).poll_read(cx, buf)
    }
}
