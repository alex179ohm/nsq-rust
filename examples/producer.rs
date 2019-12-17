use async_std::task;
use femme;
use log;
use nsq_rust::prelude::*;
use std::error::Error;

async fn my_pub(_app: ()) -> Message {
    Pub::new("test".to_owned(), b"ciao".to_vec()).into()
}

fn main() -> Result<(), Box<dyn Error>> {
    femme::start(log::LevelFilter::Debug)?;
    task::block_on(async {
        let config = Config::new();
        //let cafile = PathBuf::from("./tests/end.chain");
        let res = Client::new("localhost:4150", config, None, None)
            .publish(my_pub)
            .await;
        println!("{:?}", res);
    });
    Ok(())
}
