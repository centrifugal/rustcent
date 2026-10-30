//! A parser for the part of proto3 that Centrifugo's `api.proto` and
//! `proxy.proto` use: top-level messages with scalar, message, repeated and
//! map fields, and one service whose methods may stream.
//!
//! It accepts exactly that and rejects anything else, so that a construct it
//! does not understand - a oneof, an enum, a nested message - fails the
//! generation rather than being dropped from the SDK without a word. It is a
//! port of gocent's `internal/gen/proto.go`, which reads the same files.

use std::collections::HashMap;
use std::sync::LazyLock;

use regex::Regex;

/// What the generator needs from a proto file: its messages and the methods
/// of its service, each with the comment written above it.
#[derive(Debug, Default)]
pub(crate) struct Schema {
    pub(crate) messages: Vec<Message>,
    pub(crate) by_name: HashMap<String, usize>,
    pub(crate) methods: Vec<Method>,
}

#[derive(Debug)]
pub(crate) struct Message {
    pub(crate) name: String,
    pub(crate) doc: Vec<String>,
    pub(crate) fields: Vec<Field>,
}

#[derive(Debug, Clone)]
pub(crate) struct Field {
    /// As in the proto, snake case.
    pub(crate) name: String,
    /// The scalar or message type; the value type for maps.
    pub(crate) typ: String,
    pub(crate) repeated: bool,
    /// Set for map fields.
    pub(crate) map_key: Option<String>,
    pub(crate) doc: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct Method {
    pub(crate) request: String,
    pub(crate) response: String,
    /// Set for a method streaming its request or response: one only GRPC can
    /// call.
    pub(crate) streaming: bool,
}

pub(crate) const SCALARS: &[&str] = &[
    "string", "bool", "int32", "int64", "uint32", "uint64", "double", "float", "bytes",
];

static MESSAGE_OPEN: LazyLock<Regex> = LazyLock::new(|| re(r"^message\s+(\w+)\s*\{\s*(\})?$"));
static FIELD: LazyLock<Regex> =
    LazyLock::new(|| re(r"^(repeated\s+)?(\w+)\s+(\w+)\s*=\s*\d+\s*;$"));
static MAP_FIELD: LazyLock<Regex> =
    LazyLock::new(|| re(r"^map\s*<\s*(\w+)\s*,\s*(\w+)\s*>\s+(\w+)\s*=\s*\d+\s*;$"));
static RESERVED: LazyLock<Regex> = LazyLock::new(|| re(r"^reserved\s+[\d\s,to]+;$"));
static SERVICE_OPEN: LazyLock<Regex> = LazyLock::new(|| re(r"^service\s+\w+\s*\{$"));
static RPC: LazyLock<Regex> = LazyLock::new(|| {
    re(
        r"^rpc\s+(\w+)\s*\(\s*(stream\s+)?(\w+)\s*\)\s*returns\s*\(\s*(stream\s+)?(\w+)\s*\)\s*(?:\{\s*\}|;)$",
    )
});
static TOP_LEVEL_SKIP: LazyLock<Regex> = LazyLock::new(|| re(r"^(syntax|package|option)\b.*;$"));

fn re(s: &str) -> Regex {
    Regex::new(s).expect("a valid regular expression")
}

impl Schema {
    pub(crate) fn message(&self, name: &str) -> Option<&Message> {
        self.by_name.get(name).map(|&i| &self.messages[i])
    }
}

/// Parses a proto file; `name` is how errors refer to it.
pub(crate) fn parse(src: &str, name: &str) -> Result<Schema, String> {
    let mut s = Schema::default();
    let mut comment: Vec<String> = Vec::new();
    let mut current: Option<usize> = None;
    let mut in_service = false;
    for (i, raw) in src.lines().enumerate() {
        let line_no = i + 1;
        let fail = |msg: String| format!("{name}:{line_no}: {msg}");
        let mut line = raw.trim().to_owned();
        if line.is_empty() {
            // A blank line ends a comment block: only a comment directly
            // above a declaration documents it.
            comment.clear();
            continue;
        }
        if let Some(c) = line.strip_prefix("//") {
            comment.push(c.trim().to_owned());
            continue;
        }
        if let Some(i) = line.find("//") {
            // A trailing comment documents its line when nothing above does.
            if comment.is_empty() {
                comment.push(line[i + 2..].trim().to_owned());
            }
            line = line[..i].trim().to_owned();
        }

        if current.is_none() && !in_service {
            if TOP_LEVEL_SKIP.is_match(&line) {
                comment.clear();
                continue;
            }
            if let Some(m) = MESSAGE_OPEN.captures(&line) {
                let msg_name = m[1].to_owned();
                if s.by_name.contains_key(&msg_name) {
                    return Err(fail(format!("message {msg_name} declared twice")));
                }
                s.by_name.insert(msg_name.clone(), s.messages.len());
                s.messages.push(Message {
                    name: msg_name,
                    doc: std::mem::take(&mut comment),
                    fields: Vec::new(),
                });
                if m.get(2).is_none() {
                    current = Some(s.messages.len() - 1);
                }
                continue;
            }
            if SERVICE_OPEN.is_match(&line) {
                in_service = true;
                comment.clear();
                continue;
            }
            return Err(fail(format!("unsupported top-level declaration: {line:?}")));
        }
        if in_service {
            if line == "}" {
                in_service = false;
            } else if let Some(m) = RPC.captures(&line) {
                s.methods.push(Method {
                    request: m[3].to_owned(),
                    response: m[5].to_owned(),
                    streaming: m.get(2).is_some() || m.get(4).is_some(),
                });
            } else {
                return Err(fail(format!("unsupported service declaration: {line:?}")));
            }
            comment.clear();
            continue;
        }
        let msg = current.expect("inside a message");
        if line == "}" {
            current = None;
        } else if RESERVED.is_match(&line) {
        } else if let Some(m) = MAP_FIELD.captures(&line) {
            s.messages[msg].fields.push(Field {
                name: m[3].to_owned(),
                typ: m[2].to_owned(),
                repeated: false,
                map_key: Some(m[1].to_owned()),
                doc: std::mem::take(&mut comment),
            });
        } else if let Some(m) = FIELD.captures(&line) {
            s.messages[msg].fields.push(Field {
                name: m[3].to_owned(),
                typ: m[2].to_owned(),
                repeated: m.get(1).is_some(),
                map_key: None,
                doc: std::mem::take(&mut comment),
            });
        } else {
            return Err(fail(format!(
                "unsupported declaration in message {}: {line:?}",
                s.messages[msg].name
            )));
        }
        comment.clear();
    }
    if current.is_some() || in_service {
        return Err(format!("{name}: unexpected end of file"));
    }
    check(&s, name)?;
    Ok(s)
}

/// Verifies every message type a field or method refers to is declared.
fn check(s: &Schema, name: &str) -> Result<(), String> {
    for m in &s.messages {
        for f in &m.fields {
            if !SCALARS.contains(&f.typ.as_str()) && s.message(&f.typ).is_none() {
                return Err(format!(
                    "{name}: {}.{} has unknown type {}",
                    m.name, f.name, f.typ
                ));
            }
            if f.map_key.as_deref().is_some_and(|k| k != "string") {
                return Err(format!(
                    "{name}: {}.{}: only string map keys are supported",
                    m.name, f.name
                ));
            }
        }
    }
    for rpc in &s.methods {
        if s.message(&rpc.request).is_none() || s.message(&rpc.response).is_none() {
            return Err(format!("{name}: an rpc refers to an undeclared message"));
        }
    }
    Ok(())
}
