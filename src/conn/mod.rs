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

mod auth;
mod identify;
mod io_stream;
mod magic;
mod publish;
mod rdy;
mod subscribe;
pub mod tcp;
pub mod tls;

pub(crate) use auth::auth;
pub(crate) use identify::identify;
pub use io_stream::NsqStream;
pub(crate) use magic::magic;
pub(crate) use publish::publish;
pub(crate) use rdy::rdy;
pub(crate) use subscribe::subscribe;
