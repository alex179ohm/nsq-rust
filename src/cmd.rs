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

/// Collections of cmds defined by the nsq protocol.
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
