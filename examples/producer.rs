use nsq_rust::{Client, Config, Pub, Mpub, Dpub};
use async_std::task;
use env_logger;
use std::env;

async fn my_pub() -> Pub {
    Pub::new("test".to_owned(), b"ciao".to_vec())
}

fn main() {
    env::set_var("CARGO_LOG", "debug");
    env_logger::init();
    task::block_on(async {
        let config = Config::new();
        //let cafile = PathBuf::from("./tests/end.chain");
        if let Err(e) = Client::new("localhost:4150", config, None, None).publish(my_pub()).await {
            eprintln!("{:?}", e);
        }
    })
}
