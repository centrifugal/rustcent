//! The default HTTP client over real sockets, against small local servers:
//! connections kept alive, one deadline per request - also over a reply
//! trickling in - redirects not followed, and network errors (spec 3.2, 4.4).

#![allow(clippy::unwrap_used, clippy::pedantic)] // tests: unwrap is the assertion
#![cfg(feature = "client")]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use cent::{Client, Config, Error, InfoRequest, retryable};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// What a server writes for a request: chunks, each after a pause.
type Script = Vec<(Duration, Vec<u8>)>;

/// A server answering every request on its connections with `script`, and
/// counting the connections it accepted.
async fn server(script: impl Fn() -> Script + Send + Sync + 'static) -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let conns = Arc::new(AtomicUsize::new(0));
    let c2 = Arc::clone(&conns);
    let script = Arc::new(script);
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            c2.fetch_add(1, Ordering::SeqCst);
            let script = Arc::clone(&script);
            tokio::spawn(async move { serve_conn(stream, &*script).await });
        }
    });
    (format!("http://{addr}/api"), conns)
}

async fn serve_conn(mut stream: TcpStream, script: &(dyn Fn() -> Script + Send + Sync)) {
    let mut buf = Vec::new();
    loop {
        // Read one request: headers, then its body.
        let head_end = loop {
            if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break i + 4;
            }
            let mut chunk = [0; 4096];
            match stream.read(&mut chunk).await {
                Ok(0) | Err(_) => return,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            }
        };
        let head = String::from_utf8_lossy(&buf[..head_end]).to_ascii_lowercase();
        let len: usize = head
            .lines()
            .find_map(|l| l.strip_prefix("content-length:"))
            .map_or(0, |v| v.trim().parse().unwrap());
        while buf.len() < head_end + len {
            let mut chunk = [0; 4096];
            match stream.read(&mut chunk).await {
                Ok(0) | Err(_) => return,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            }
        }
        buf.drain(..head_end + len);
        for (pause, bytes) in script() {
            tokio::time::sleep(pause).await;
            if stream.write_all(&bytes).await.is_err() {
                return;
            }
        }
    }
}

fn reply(status: &str, body: &str) -> Script {
    vec![(
        Duration::ZERO,
        format!(
            "HTTP/1.1 {status}\r\ncontent-length: {}\r\n\r\n{body}",
            body.len()
        )
        .into_bytes(),
    )]
}

fn client(endpoint: &str, timeout: Duration) -> Client {
    Client::new(
        Config::new(endpoint)
            .api_key("key")
            .request_timeout(timeout),
    )
    .unwrap()
}

#[tokio::test]
async fn connections_are_kept_alive() {
    let (endpoint, conns) = server(|| reply("200 OK", r#"{"result":{}}"#)).await;
    let client = client(&endpoint, Duration::from_secs(5));
    for _ in 0..5 {
        client.info(InfoRequest::new()).await.unwrap();
    }
    assert_eq!(conns.load(Ordering::SeqCst), 1);
    // An error status with a small body keeps the connection too: it is
    // drained, not dropped.
    let (endpoint, conns) =
        server(|| reply("500 Internal Server Error", &"x".repeat(10_000))).await;
    let client = self::client(&endpoint, Duration::from_secs(5));
    for _ in 0..3 {
        let e = client.info(InfoRequest::new()).await.unwrap_err();
        assert!(matches!(&e, Error::Http(h) if h.body.len() == 512), "{e:?}");
    }
    assert_eq!(conns.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_reply_trickling_in_ends_at_the_deadline() {
    // Headers at once, then the body a byte at a time.
    let (endpoint, _) = server(|| {
        let mut s = vec![(
            Duration::ZERO,
            b"HTTP/1.1 200 OK\r\ncontent-length: 100\r\n\r\n".to_vec(),
        )];
        s.extend((0..100).map(|_| (Duration::from_millis(50), b" ".to_vec())));
        s
    })
    .await;
    let start = Instant::now();
    let e = client(&endpoint, Duration::from_millis(300))
        .info(InfoRequest::new())
        .await
        .unwrap_err();
    assert!(e.is_timeout() && retryable(&e), "{e:?}");
    assert!(
        start.elapsed() < Duration::from_secs(1),
        "{:?}",
        start.elapsed()
    );

    // The headers themselves trickling in.
    let (endpoint, _) = server(|| {
        b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\n{}"
            .iter()
            .map(|b| (Duration::from_millis(50), vec![*b]))
            .collect()
    })
    .await;
    let start = Instant::now();
    let e = client(&endpoint, Duration::from_millis(300))
        .info(InfoRequest::new())
        .await
        .unwrap_err();
    assert!(e.is_timeout(), "{e:?}");
    assert!(start.elapsed() < Duration::from_secs(1));
}

#[tokio::test]
async fn redirects_are_not_followed() {
    let (endpoint, conns) = server(|| {
        vec![(Duration::ZERO, b"HTTP/1.1 301 Moved Permanently\r\nlocation: https://elsewhere/api/info\r\ncontent-length: 0\r\n\r\n".to_vec())]
    })
    .await;
    let e = client(&endpoint, Duration::from_secs(5))
        .info(InfoRequest::new())
        .await
        .unwrap_err();
    let Error::Http(h) = &e else { panic!("{e:?}") };
    assert_eq!(h.status, 301);
    assert!(
        e.to_string()
            .contains("a redirect, which the client does not follow")
    );
    assert!(!retryable(&e));
    assert_eq!(conns.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn refused_connections_are_retryable() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let e = client(&format!("http://{addr}/api"), Duration::from_secs(5))
        .info(InfoRequest::new())
        .await
        .unwrap_err();
    assert!(
        matches!(&e, Error::Network(n) if !n.is_certificate()),
        "{e:?}"
    );
    assert!(retryable(&e));
}

#[tokio::test]
async fn refused_certificates_are_not_retryable() {
    // A self-signed certificate for localhost, valid until 2126, for this
    // test only: no trust store has it.
    use rustls::pki_types::pem::PemObject as _;
    let cert =
        rustls::pki_types::CertificateDer::from_pem_slice(include_bytes!("testdata/localhost.crt"))
            .unwrap();
    let key =
        rustls::pki_types::PrivateKeyDer::from_pem_slice(include_bytes!("testdata/localhost.key"))
            .unwrap();
    let tls = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(vec![cert], key)
    .unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(tls));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                let _ = acceptor.accept(stream).await;
            });
        }
    });
    let e = client(
        &format!("https://localhost:{port}/api"),
        Duration::from_secs(5),
    )
    .info(InfoRequest::new())
    .await
    .unwrap_err();
    assert!(
        matches!(&e, Error::Network(n) if n.is_certificate()),
        "{e:?}"
    );
    assert!(!retryable(&e));
}

#[tokio::test]
async fn close_drops_idle_connections() {
    let (endpoint, _) = server(|| reply("200 OK", r#"{"result":{}}"#)).await;
    let client = client(&endpoint, Duration::from_secs(5));
    client.info(InfoRequest::new()).await.unwrap();
    client.close(Duration::from_secs(1)).await.unwrap();
    assert!(matches!(
        client.info(InfoRequest::new()).await,
        Err(Error::Closed)
    ));
}
