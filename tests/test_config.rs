
use nsq_rust::prelude::*;

#[test]
fn config_builder_new_test() {
    let config: Config = ConfigBuilder::new().into();
    println!("{:?}", config);
}
