use async_std::task;
use nsq_rust::client;
use nsq_rust::prelude::*;
use std::collections::HashMap;
use std::error::Error;

#[derive(Debug)]
struct App {
    pub db: HashMap<String, String>,
}

impl App {
    fn new() -> App {
        App { db: HashMap::new() }
    }
}

async fn my_pub(app: App) -> Message {
    let topic = "test";
    let msg = app.db.get(&topic.to_owned()).unwrap();

    Pub::with_topic_msg(topic, msg.as_bytes()).into()
}

fn main() -> Result<(), Box<dyn Error>> {
    femme::start(log::LevelFilter::Trace)?;

    let config: Config = ConfigBuilder::new().into();
    let mut app = App::new();

    let _ = app.db.insert("test".to_owned(), "msg".to_owned());

    task::block_on(async move {
        let clnt = client::Builder::new()
            .addr("localhost:4150")
            .config(config)
            .build_with_state(app);

        log::debug!("client created: {:?}", clnt);

        let res = clnt.publish(my_pub).await;
        log::trace!("{:?}", res);
    });

    Ok(())
}
