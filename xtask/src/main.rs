//! Maintainer tasks for rustcent: `cargo xtask --help`.
//!
//! - `cargo xtask generate` writes `crates/cent/src/generated/` from
//!   `proto/api.proto` and `proto/proxy.proto`: the request and result types
//!   of every API method, the client, batch, command and blocking client
//!   methods sending them, and the proxy protocol's types.
//! - `cargo xtask generate --check` fails when the generated files are stale.
//! - `cargo xtask sync-proto <tag>` copies the protos from a released
//!   Centrifugo tag, then generates.

#![allow(clippy::print_stdout, clippy::print_stderr)]

mod generate;
mod proto;

use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args.first().map(String::as_str) {
        Some("generate") => {
            let check = args.iter().any(|a| a == "--check");
            let accept = args.iter().any(|a| a == "--accept-required");
            generate::run(&root(), check, accept)
        }
        Some("sync-proto") => match args.get(1) {
            Some(r) => sync_proto(r),
            None => Err("usage: cargo xtask sync-proto <centrifugo tag, such as v6.10.2>".into()),
        },
        _ => {
            println!(
                "cargo xtask generate [--check] [--accept-required]   write crates/cent/src/generated/ from proto/\n\
                 cargo xtask sync-proto <tag>                         copy the protos from a Centrifugo tag, then generate"
            );
            return ExitCode::SUCCESS;
        }
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("xtask: {e}");
            ExitCode::FAILURE
        }
    }
}

/// The workspace root.
fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask lives in the workspace")
        .to_owned()
}

/// Copies api.proto and proxy.proto from Centrifugo's tag `r` - always a
/// released tag before a release - and generates.
fn sync_proto(r: &str) -> Result<(), String> {
    let root = root();
    let base = format!("https://raw.githubusercontent.com/centrifugal/centrifugo/{r}/internal");
    for (src, dst, service) in [
        (
            "apiproto/api.proto",
            "proto/api.proto",
            "service CentrifugoApi",
        ),
        (
            "proxyproto/proxy.proto",
            "proto/proxy.proto",
            "service CentrifugoProxy",
        ),
    ] {
        let out = Command::new("curl")
            .args(["-fsSL", &format!("{base}/{src}")])
            .output()
            .map_err(|e| format!("running curl: {e}"))?;
        if !out.status.success() {
            return Err(format!("no {src} at Centrifugo {r}"));
        }
        let text = String::from_utf8(out.stdout).map_err(|e| e.to_string())?;
        if !text.lines().any(|l| l.starts_with(service)) {
            return Err(format!("the file at Centrifugo {r} is not its {dst}"));
        }
        std::fs::write(root.join(dst), text).map_err(|e| e.to_string())?;
    }
    generate::run(&root, false, false)?;
    println!("protos are Centrifugo {r}'s: review git diff, then run the checks");
    Ok(())
}
