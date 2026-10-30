//! The blocking client, for code without an async runtime: the same methods,
//! returning results.
//!
//! ```sh
//! CENTRIFUGO_API_KEY=api_key cargo run --example blocking --features blocking
//! ```

#![allow(clippy::print_stdout, clippy::print_stderr)] // an example shows what happened

use std::time::Duration;

use cent::{Config, Error, InfoRequest, PublishRequest, blocking};

fn main() -> Result<(), Error> {
    let endpoint =
        std::env::var("CENTRIFUGO_API").unwrap_or_else(|_| "http://localhost:8000/api".into());
    let key = std::env::var("CENTRIFUGO_API_KEY").unwrap_or_default();
    let client = blocking::Client::new(Config::new(endpoint).api_key(key))?;

    let info = client
        .with_timeout(Duration::from_secs(2))
        .info(InfoRequest::new())?;
    println!("{} Centrifugo nodes", info.nodes.len());

    let data = cent::json(&serde_json::json!({"text": "hello"}))?;
    let res = client.publish(PublishRequest::new("news").data(data))?;
    println!("published at offset {}", res.offset);

    client.close(Duration::from_secs(10))
}
