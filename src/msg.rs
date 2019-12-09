
#[derive(Debug)]
pub struct Msg {
    timestamp: i64,
    attemps: u16,
    id: String,
    body: Vec<u8>,
}

impl Into<Msg> for (i64, u16, String, Vec<u8>) {
    fn into(self) -> Msg {
        Msg {
            timestamp: self.0,
            attemps: self.1,
            id: self.2,
            body: self.3,
        }
    }
}
