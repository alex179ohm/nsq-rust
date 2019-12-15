use crate::codec::{Cls, Encoder, Fin, Req, Touch};
use bytes::BytesMut;

pub struct Cmd(BytesMut);

impl From<Touch> for Cmd {
    fn from(t: Touch) -> Cmd {
        let mut buf = BytesMut::new();
        t.encode(&mut buf);
        Cmd(buf)
    }
}

impl From<Fin> for Cmd {
    fn from(f: Fin) -> Cmd {
        let mut buf = BytesMut::new();
        f.encode(&mut buf);
        Cmd(buf)
    }
}

impl From<Req> for Cmd {
    fn from(r: Req) -> Cmd {
        let mut buf = BytesMut::new();
        r.encode(&mut buf);
        Cmd(buf)
    }
}

impl From<Cls> for Cmd {
    fn from(c: Cls) -> Cmd {
        let mut buf = BytesMut::new();
        c.encode(&mut buf);
        Cmd(buf)
    }
}
