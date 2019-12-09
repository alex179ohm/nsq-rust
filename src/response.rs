use std::fmt;
use crate::msg::Msg;

pub enum Response {
    HeartBeat,
    Ok,
    Msg(Msg),
    Json(String),
}

impl Into<Response> for &'_ str {
    fn into(self) -> Response {
        match self {
            "OK" => Response::Ok,
            "CLOSE_WAIT" => Response::Ok,
            "__heartbeat__" => Response::HeartBeat,
            s => Response::Json(String::from(s)),
        }
    }
}

impl Into<Response> for (i64, u16, String, Vec<u8>) {
    fn into(self) -> Response {
        Response::Msg(self.into())
    }
}

impl Into<Response> for Msg {
    fn into(self) -> Response {
        Response::Msg(self)
    }
}

impl fmt::Debug for Response {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use Response::*;
        match self {
            Ok => write!(f, "OK"),
            HeartBeat => write!(f, "HEARTBEAT"),
            Msg(m) => write!(f, "{:?}", m),
            Json(s) => write!(f, "{:?}", s),
        }
    }
}
