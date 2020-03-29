use std::pin::Pin;
use std::task::{Context, Poll};

use futures_io::{AsyncRead, AsyncWrite};

pub(crate) enum Stream<S, T> {
    Plain(S),
    Tls(T),
}

impl<S: AsyncRead + Unpin, T: AsyncRead + Unpin> AsyncRead for Stream<S, T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        match self.get_mut() {
            Stream::Plain(ref mut s) => Pin::new(s).poll_read(cx, buf),
            Stream::Tls(ref mut s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl<S: AsyncWrite + Unpin, T: AsyncWrite + Unpin> AsyncWrite for Stream<S, T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        match self.get_mut() {
            Stream::Plain(ref mut s) => Pin::new(s).poll_write(cx, buf),
            Stream::Tls(ref mut s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        match self.get_mut() {
            Stream::Plain(ref mut s) => Pin::new(s).poll_flush(cx),
            Stream::Tls(ref mut s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        match self.get_mut() {
            Stream::Plain(ref mut s) => Pin::new(s).poll_close(cx),
            Stream::Tls(ref mut s) => Pin::new(s).poll_close(cx),
        }
    }
}
