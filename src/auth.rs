use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Authentication {
    identity: String,
    identity_url: Option<String>,
    permission_count: i32,
}
