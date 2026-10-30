//! Publishes, broadcasts and sends a batch, handling each outcome; then
//! closes the client, as on shutdown.
//!
//! ```sh
//! CENTRIFUGO_API_KEY=api_key cargo run --example publish
//! ```

#![allow(clippy::print_stdout, clippy::print_stderr)] // an example shows what happened

use std::time::Duration;

use cent::{
    BatchOptions, BroadcastRequest, BroadcastRetryOptions, Client, Config, Error, PublishRequest,
};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let endpoint =
        std::env::var("CENTRIFUGO_API").unwrap_or_else(|_| "http://localhost:8000/api".into());
    let key = std::env::var("CENTRIFUGO_API_KEY").unwrap_or_default();
    // Created once, and shared: it is cheap to clone.
    let client = Client::new(Config::new(endpoint).api_key(key))?;

    // A payload is JSON: encode any serde type with cent::json, or pass JSON
    // you already have with RawValue::from_string.
    let data = cent::json(&json!({"text": "hello"}))?;
    let res = client
        .publish(PublishRequest::new("news").data(data.clone()))
        .await?;
    println!("published at offset {} in epoch {}", res.offset, res.epoch);

    // A broadcast: every channel's outcome, and an error unless all got it.
    let request = BroadcastRequest::new(["user:1", "user:2", "unknown:3"]).data(data.clone());
    match client
        .broadcast_with_retry(request, BroadcastRetryOptions::default())
        .await
    {
        Ok(res) => println!("broadcast into {} channels", res.channels.len()),
        Err(Error::BroadcastFailedChannels(e)) => {
            for ch in e.failed() {
                if let Err(err) = &ch.result {
                    eprintln!("not published into {}: {err}", ch.channel);
                }
            }
        }
        Err(e) => return Err(e), // no channel got a reply
    }

    // A batch: one request, each command its own outcome.
    let mut batch = client.new_batch(BatchOptions::default());
    let publication = batch.publish(PublishRequest::new("news").data(data));
    let stats = batch.presence_stats(cent::PresenceStatsRequest::new("news"));
    if let Err(e) = batch.send().await {
        eprintln!("batch: {e}");
    }
    println!(
        "offset {:?}, stats {:?}",
        publication.result().map(|r| r.offset),
        stats.result().map(|s| s.num_clients)
    );

    // On SIGTERM: refuse new calls, and wait for those in progress.
    client.close(Duration::from_secs(10)).await
}
