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

use crate::codec::{Dpub, Encoder, Mpub, Pub};
use bytes::BytesMut;
use futures_core::future::Future;
use futures_util::future::BoxFuture;

pub struct Message(BytesMut);

impl From<Pub<'_>> for Message {
    fn from(p: Pub<'_>) -> Self {
        Message(p.encode())
    }
}

impl From<Dpub<'_>> for Message {
    fn from(d: Dpub<'_>) -> Self {
        Message(d.encode())
    }
}

impl From<Mpub<'_>> for Message {
    fn from(m: Mpub<'_>) -> Self {
        Message(m.encode())
    }
}

pub trait Publisher<State>: Send + Sync + 'static {
    type Fut: Future<Output = Message> + Send + 'static;
    fn call(&self, s: State) -> Self::Fut;
}

impl<F: Send + Sync + 'static, Fut, State> Publisher<State> for F
where
    F: Fn(State) -> Fut,
    Fut: Future + Send + Sync + 'static,
    Fut::Output: Into<Message>,
{
    type Fut = BoxFuture<'static, Message>;
    fn call(&self, s: State) -> Self::Fut {
        let fut = (self)(s);
        Box::pin(async move { fut.await.into() })
    }
}
