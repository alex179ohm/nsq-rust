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

use crate::cmd::Cmd;
use crate::msg::Msg;
use futures::future::BoxFuture;
use futures::Future;

pub trait Consumer: Send + Sync + 'static {
    type Fut: Future<Output = Cmd> + Send + 'static;
    fn call(&self, cx: Msg) -> Self::Fut;
}

pub(crate) type DynConsumer = dyn (Fn(Msg) -> BoxFuture<'static, Cmd>) + Send + Sync + 'static;

impl<F: Send + Sync + 'static, Fut> Consumer for F
where
    F: Fn(Msg) -> Fut,
    Fut: Future + Send + 'static,
    Fut::Output: Into<Cmd>,
{
    type Fut = BoxFuture<'static, Cmd>;
    fn call(&self, cx: Msg) -> Self::Fut {
        let fut = (self)(cx);
        Box::pin(async move { fut.await.into() })
    }
}

pub trait TopicConsumer: Send + Sync + 'static {
    type Fut: Future<Output = Cmd> + Send + 'static;
    fn call(&self, topic: String, channel: String, cx: Msg) -> Self::Fut;
}

//pub(crate) type DynTopicConsumer = dyn (Fn(String, String, Msg) -> BoxFuture<'static, Cmd>) + Send + Sync + 'static;

impl<F: Send + Sync + 'static, Fut> TopicConsumer for F
where
    F: Fn(String, String, Msg) -> Fut,
    Fut: Future + Send + Sync + 'static,
    Fut::Output: Into<Cmd>,
{
    type Fut = BoxFuture<'static, Cmd>;
    fn call(&self, topic: String, channel: String, cx: Msg) -> Self::Fut {
        let fut = (self)(topic, channel, cx);
        Box::pin(async move { fut.await.into() })
    }
}
