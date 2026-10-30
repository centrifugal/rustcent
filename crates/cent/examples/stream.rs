//! Streams a model's output into a channel: what arrived every 50 ms, one
//! publication at a time - each waiting for the previous one, so that they
//! keep their order - into a channel of its own, in a namespace with history
//! and recovery, so that a client joining late or reconnecting gets the whole
//! response. See <https://centrifugal.dev/docs/getting-started/ai_apps>.
//!
//! ```sh
//! CENTRIFUGO_API_KEY=api_key cargo run --example stream
//! ```

#![allow(clippy::print_stdout, clippy::print_stderr)] // an example shows what happened

use std::time::Duration;

use cent::{Client, Config, Error, PublishRequest};
use serde::Serialize;
use tokio::sync::mpsc;

#[derive(Serialize)]
struct Chunk<'a> {
    text: &'a str,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    done: bool,
}

/// Publishes the tokens of one response into `channel`, until the model
/// closes `tokens`.
async fn stream(
    client: &Client,
    channel: &str,
    mut tokens: mpsc::Receiver<String>,
) -> Result<(), Error> {
    let mut text = String::new();
    let mut seq = 0;
    let mut tick = tokio::time::interval(Duration::from_millis(50));
    loop {
        let done = tokio::select! {
            token = tokens.recv() => match token {
                Some(t) => { text.push_str(&t); continue; }
                None => true,
            },
            _ = tick.tick() => false,
        };
        if text.is_empty() && !done {
            continue; // the model is thinking, or calling a tool
        }
        seq += 1;
        let data = cent::json(&Chunk { text: &text, done })?;
        client
            .publish(
                PublishRequest::new(channel)
                    .data(data)
                    // Retried with the same key, a chunk is not published twice.
                    .idempotency_key(format!("{channel}-{seq}")),
            )
            .await?;
        text.clear();
        if done {
            return Ok(());
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let endpoint =
        std::env::var("CENTRIFUGO_API").unwrap_or_else(|_| "http://localhost:8000/api".into());
    let key = std::env::var("CENTRIFUGO_API_KEY").unwrap_or_default();
    let client = Client::new(Config::new(endpoint).api_key(key))?;
    let (tx, rx) = mpsc::channel(64);
    // The model: tokens arriving over a second.
    tokio::spawn(async move {
        for word in "Centrifugo delivers this answer as it is written.".split_inclusive(' ') {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = tx.send(word.to_owned()).await;
        }
    });
    stream(&client, "ai:response-42", rx).await
}
