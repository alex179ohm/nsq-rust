use crate::error::NsqError;
use crate::response::Response;


pub type NsqResult<T> = Result<T, NsqError>;

impl Into<NsqResult<Response>> for Response {
    fn into(self) -> NsqResult<Response> {
        NsqResult::Ok(self)
    }
}

impl Into<NsqResult<Response>> for NsqError {
    fn into(self) -> NsqResult<Response> {
        NsqResult::Err(self)
    }
}
