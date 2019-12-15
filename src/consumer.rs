use futures::future::BoxFuture;
use futures::Future;
use crate::cmd::Cmd;
use crate::msg::Msg;

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

