use async_std::task;
use nsq_rust::client;
use nsq_rust::prelude::*;
use std::error::Error;

async fn my_pub(_app: ()) -> Message {
    Pub::with_topic_msg("test", &b"ciao"[..]).into()
}

fn main() -> Result<(), Box<dyn Error>> {
    femme::start(log::LevelFilter::Debug)?;
    let config: Config = ConfigBuilder::new().into();
    //let cafile = PathBuf::from("./tests/end.chain");
    task::block_on(async move {
        let clnt = client::Builder::new()
            .addr("http://localhost:3000")
            .config(config)
            //.cafile(cafile)
            .build();

        let res = clnt.publish(my_pub).await;
        println!("{:?}", res);
    });
    Ok(())
}
