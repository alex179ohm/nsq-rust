use async_std::task;
use femme;
use log;
use nsq_rust::prelude::*;
use std::collections::HashMap;
use std::error::Error;

struct App {
    pub db: HashMap<String, String>,
}

impl App {
    fn new() -> App {
        App { db: HashMap::new() }
    }
}

async fn my_pub(app: App) -> Message {
    let topic = "test".to_owned();
    let msg = app.db.get(&topic).unwrap();
    Pub::new(topic, msg.as_bytes().to_vec()).into()
}

fn main() -> Result<(), Box<dyn Error>> {
    femme::start(log::LevelFilter::Trace)?;
    let config = Config::new();
    let mut app = App::new();
    let _ = app.db.insert("test".to_owned(), "msg".to_owned());
    task::block_on(async move {
        let res = Client::with_state("localhost:4150", config, None, None, app)
            .publish(my_pub)
            .await;
        log::trace!("{:?}", res);
    });
    Ok(())
}
