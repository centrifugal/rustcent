//! Writes `crates/cent/src/generated/{api,proxy}.rs` from the protos.

use std::collections::{BTreeSet, HashSet};
use std::fmt::Write as _;
use std::io::Write as _;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::LazyLock;

use regex::Regex;

use crate::proto::{self, Field, Message, SCALARS, Schema};

/// API commands the SDK does not offer: `rpc` calls server API extensions,
/// which Centrifugo does not provide (spec 1).
const OMITTED_COMMANDS: &[&str] = &["rpc"];

/// Requests with rules the client checks before sending (spec 3.5), each
/// with a function of `crate::validate`.
const VALIDATED: &[&str] = &["publish", "broadcast"];

/// Messages of the API written by hand, or only used on the wire, where the
/// crate has its own form of them.
const API_HANDWRITTEN: &[&str] = &[
    "Error",
    "BroadcastResult",
    "Command",
    "Reply",
    "BatchRequest",
    "BatchResponse",
];

/// Proxy messages whose constructor is written by hand: `Error::new(code,
/// message)` and `Disconnect::new(code, reason)`.
const PROXY_OWN_CONSTRUCTOR: &[&str] = &["Error", "Disconnect"];

/// Rust keywords among proto field names: written as raw identifiers.
const KEYWORDS: &[&str] = &[
    "as", "async", "await", "box", "break", "const", "continue", "crate", "dyn", "else", "enum",
    "extern", "false", "final", "fn", "for", "gen", "if", "impl", "in", "let", "loop", "macro",
    "match", "mod", "move", "mut", "override", "priv", "pub", "ref", "return", "static", "struct",
    "super", "trait", "true", "try", "type", "typeof", "unsafe", "unsized", "use", "virtual",
    "where", "while", "yield",
];

pub(crate) fn run(root: &Path, check: bool, accept_required: bool) -> Result<(), String> {
    let read =
        |p: &str| std::fs::read_to_string(root.join(p)).map_err(|e| format!("reading {p}: {e}"));
    let mut api = proto::parse(&read("proto/api.proto")?, "api.proto")?;
    let mut proxy = proto::parse(&read("proto/proxy.proto")?, "proxy.proto")?;
    rust_type_names(&mut api);
    rust_type_names(&mut proxy);

    let mut required = Reviewed::default();
    let api_src = generate_api(&api, &mut required)?;
    let proxy_src = generate_proxy(&proxy)?;
    let test_src = generate_methods_test(&api)?;
    check_required(root, &required, accept_required)?;

    let mut stale = Vec::new();
    for (path, src) in [
        ("crates/cent/src/generated/api.rs", api_src),
        ("crates/cent/src/generated/proxy.rs", proxy_src),
        ("crates/cent/tests/methods.rs", test_src),
    ] {
        let formatted = rustfmt(&src)?;
        let full = root.join(path);
        if check {
            if std::fs::read_to_string(&full).ok().as_deref() != Some(formatted.as_str()) {
                stale.push(path);
            }
        } else {
            std::fs::write(&full, formatted).map_err(|e| format!("writing {path}: {e}"))?;
        }
    }
    if !stale.is_empty() {
        return Err(format!(
            "stale: {} - run cargo xtask generate",
            stale.join(", ")
        ));
    }
    Ok(())
}

/// Writes acronyms in type names as Rust does: `RPCRequest` is `RpcRequest`.
fn rust_type_names(s: &mut Schema) {
    let rename = |n: &mut String| *n = n.replace("RPC", "Rpc");
    s.by_name.clear();
    for (i, m) in s.messages.iter_mut().enumerate() {
        rename(&mut m.name);
        s.by_name.insert(m.name.clone(), i);
        for f in &mut m.fields {
            rename(&mut f.typ);
        }
    }
    for rpc in &mut s.methods {
        rename(&mut rpc.request);
        rename(&mut rpc.response);
    }
}

/// What a person reviews before it changes, because a change breaks
/// applications - without a compile error, or with one they did not expect.
#[derive(Default)]
pub(crate) struct Reviewed {
    /// The required fields, as `Message.field`.
    fields: BTreeSet<String>,
    /// The constructors with several required arguments, as `Message(a, b)`:
    /// their order is the protos' field order, and a proto reordering fields
    /// of the same type would swap what callers pass without a compile error.
    constructors: BTreeSet<String>,
}

/// The fields the protos mark plainly "Required." are required arguments of
/// their message's `new`. They are checked against `xtask/required_fields.txt`,
/// the reviewed list the other SDKs share: a proto comment gaining "Required."
/// would break every call without the field, so it is reviewed first. The
/// order of several required arguments is checked against
/// `xtask/constructors.txt`.
fn check_required(root: &Path, got: &Reviewed, accept: bool) -> Result<(), String> {
    check_list(
        root,
        "xtask/required_fields.txt",
        &got.fields,
        accept,
        "fields the protos mark Required.",
    )?;
    check_list(
        root,
        "xtask/constructors.txt",
        &got.constructors,
        accept,
        "constructors with several required arguments, or their order,",
    )
}

fn check_list(
    root: &Path,
    file: &str,
    got: &BTreeSet<String>,
    accept: bool,
    what: &str,
) -> Result<(), String> {
    let path = root.join(file);
    if accept {
        let text: String = got.iter().flat_map(|f| [f.as_str(), "\n"]).collect();
        return std::fs::write(&path, text).map_err(|e| e.to_string());
    }
    let have: BTreeSet<String> = std::fs::read_to_string(&path)
        .map_err(|e| format!("reading {file}: {e}"))?
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_owned)
        .collect();
    if &have == got {
        return Ok(());
    }
    let added: Vec<_> = got.difference(&have).cloned().collect();
    let removed: Vec<_> = have.difference(got).cloned().collect();
    let mut msg = format!("the {what} changed:");
    if !added.is_empty() {
        let _ = write!(msg, "\n  new (breaking for callers): {}", added.join(", "));
    }
    if !removed.is_empty() {
        let _ = write!(msg, "\n  gone: {}", removed.join(", "));
    }
    let _ = write!(
        msg,
        "\nReview them in {file} - fix the proto upstream if Centrifugo accepts a field absent - then run cargo xtask generate --accept-required"
    );
    Err(msg)
}

fn rustfmt(src: &str) -> Result<String, String> {
    let mut child = Command::new("rustfmt")
        .args(["--edition", "2024", "--emit", "stdout", "--quiet"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("running rustfmt: {e}"))?;
    child
        .stdin
        .take()
        .expect("piped stdin")
        .write_all(src.as_bytes())
        .map_err(|e| e.to_string())?;
    let out = child.wait_with_output().map_err(|e| e.to_string())?;
    if !out.status.success() {
        let dump = std::env::temp_dir().join("rustcent-generated.rs");
        let _ = std::fs::write(&dump, src);
        return Err(format!(
            "rustfmt failed on the generated code, written to {}:\n{}",
            dump.display(),
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    String::from_utf8(out.stdout).map_err(|e| e.to_string())
}

// ---------------------------------------------------------------------------
// Types

/// How a message is used, which decides what is generated for it.
#[derive(Clone, Copy)]
struct Usage {
    /// Built by applications and sent: it gets `new` and setters.
    input: bool,
}

/// A message holding a single `value` field, like `BoolValue`: tri-state,
/// absent leaving the server's setting alone (spec 2). It is an `Option` of
/// its scalar.
fn wrapper_scalar<'a>(s: &'a Schema, typ: &str) -> Option<&'a str> {
    let m = s.message(typ)?;
    (m.name.ends_with("Value") && m.fields.len() == 1 && m.fields[0].name == "value")
        .then(|| m.fields[0].typ.as_str())
}

fn scalar_type(typ: &str) -> &'static str {
    match typ {
        "string" => "String",
        "bool" => "bool",
        "int32" => "i32",
        "int64" => "i64",
        "uint32" => "u32",
        "uint64" => "u64",
        "double" => "f64",
        "float" => "f32",
        "bytes" => "Box<RawValue>",
        _ => unreachable!("not a scalar: {typ}"),
    }
}

/// The type of a single value of `typ`, as a list element or map value.
fn element_type(s: &Schema, typ: &str) -> Result<String, String> {
    if SCALARS.contains(&typ) {
        return Ok(scalar_type(typ).to_owned());
    }
    if wrapper_scalar(s, typ).is_some() {
        return Err(format!("a list or map of {typ} is not supported"));
    }
    Ok(typ.to_owned())
}

/// The Rust type of a field, and its serde attribute.
fn field_type(s: &Schema, f: &Field) -> Result<(String, &'static str), String> {
    if f.map_key.is_some() {
        let v = element_type(s, &f.typ)?;
        return Ok((
            format!("BTreeMap<String, {v}>"),
            r#"#[serde(default, skip_serializing_if = "BTreeMap::is_empty", deserialize_with = "crate::wire::nullable")]"#,
        ));
    }
    if f.repeated {
        let v = element_type(s, &f.typ)?;
        return Ok((
            format!("Vec<{v}>"),
            r#"#[serde(default, skip_serializing_if = "Vec::is_empty", deserialize_with = "crate::wire::nullable")]"#,
        ));
    }
    if f.typ == "bytes" {
        // A payload: absent is None, JSON null is Some(null) (spec 3.4).
        return Ok((
            "Option<Box<RawValue>>".into(),
            r#"#[serde(default, skip_serializing_if = "Option::is_none", deserialize_with = "crate::wire::payload")]"#,
        ));
    }
    if SCALARS.contains(&f.typ.as_str()) {
        return Ok((
            scalar_type(&f.typ).into(),
            r#"#[serde(default, skip_serializing_if = "crate::wire::is_default", deserialize_with = "crate::wire::nullable")]"#,
        ));
    }
    if let Some(inner) = wrapper_scalar(s, &f.typ) {
        return Ok((
            format!("Option<{}>", scalar_type(inner)),
            r#"#[serde(default, skip_serializing_if = "Option::is_none", with = "crate::wire::wrapped")]"#,
        ));
    }
    Ok((
        format!("Option<{}>", f.typ),
        r#"#[serde(default, skip_serializing_if = "Option::is_none")]"#,
    ))
}

/// How a setter takes a field's value: generics, the parameter's type, and
/// the expression storing it.
struct Setter {
    generics: &'static str,
    param: String,
    store: &'static str,
}

fn setter(s: &Schema, f: &Field) -> Result<Setter, String> {
    let plain = |param: String| Setter {
        generics: "",
        param,
        store: "value",
    };
    if f.map_key.is_some() {
        let v = element_type(s, &f.typ)?;
        return Ok(if f.typ == "string" {
            Setter {
                generics: "<K: Into<String>, V: Into<String>>",
                param: "impl IntoIterator<Item = (K, V)>".into(),
                store: "value.into_iter().map(|(k, v)| (k.into(), v.into())).collect()",
            }
        } else {
            Setter {
                generics: "<K: Into<String>>",
                param: format!("impl IntoIterator<Item = (K, {v})>"),
                store: "value.into_iter().map(|(k, v)| (k.into(), v)).collect()",
            }
        });
    }
    if f.repeated {
        let v = element_type(s, &f.typ)?;
        return Ok(if f.typ == "string" {
            Setter {
                generics: "",
                param: "impl IntoIterator<Item = impl Into<String>>".into(),
                store: "value.into_iter().map(Into::into).collect()",
            }
        } else {
            Setter {
                generics: "",
                param: format!("impl IntoIterator<Item = {v}>"),
                store: "value.into_iter().collect()",
            }
        });
    }
    if f.typ == "string" {
        return Ok(Setter {
            generics: "",
            param: "impl Into<String>".into(),
            store: "value.into()",
        });
    }
    if f.typ == "bytes" {
        return Ok(Setter {
            generics: "",
            param: "Box<RawValue>".into(),
            store: "Some(value)",
        });
    }
    if SCALARS.contains(&f.typ.as_str()) {
        return Ok(plain(scalar_type(&f.typ).into()));
    }
    if let Some(inner) = wrapper_scalar(s, &f.typ) {
        return Ok(Setter {
            generics: "",
            param: scalar_type(inner).into(),
            store: "Some(value)",
        });
    }
    Ok(Setter {
        generics: "",
        param: f.typ.clone(),
        store: "Some(value)",
    })
}

/// The expression of a field's zero value.
fn empty_value(s: &Schema, f: &Field) -> Result<&'static str, String> {
    let (typ, _) = field_type(s, f)?;
    Ok(match typ.as_str() {
        _ if typ.starts_with("Option<") => "None",
        _ if typ.starts_with("Vec<") => "Vec::new()",
        _ if typ.starts_with("BTreeMap<") => "BTreeMap::new()",
        "String" => "String::new()",
        "bool" => "false",
        "f32" | "f64" => "0.0",
        _ => "0",
    })
}

fn ident(name: &str) -> String {
    if KEYWORDS.contains(&name) {
        format!("r#{name}")
    } else {
        name.to_owned()
    }
}

/// A field the proto marks plainly "Required." - not "Required unless ..." -
/// is a required argument of `new` (spec 3.3).
fn is_required(f: &Field) -> bool {
    static REQUIRED: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"\bRequired\.").expect("regex"));
    REQUIRED.is_match(&f.doc.join(" "))
}

/// The messages reachable from `roots` through their fields.
fn closure<'a>(s: &Schema, roots: impl IntoIterator<Item = &'a str>) -> HashSet<String> {
    let mut seen = HashSet::new();
    let mut stack: Vec<String> = roots.into_iter().map(str::to_owned).collect();
    while let Some(name) = stack.pop() {
        if !seen.insert(name.clone()) {
            continue;
        }
        if let Some(m) = s.message(&name) {
            for f in &m.fields {
                if s.message(&f.typ).is_some() && !seen.contains(&f.typ) {
                    stack.push(f.typ.clone());
                }
            }
        }
    }
    seen
}

struct Writer {
    out: String,
}

impl Writer {
    fn line(&mut self, s: &str) {
        self.out.push_str(s);
        self.out.push('\n');
    }

    fn doc(&mut self, lines: &[String]) {
        for l in lines {
            if l.is_empty() {
                self.line("///");
            } else {
                self.line(&format!("/// {l}"));
            }
        }
    }
}

/// Escapes proto comment text for rustdoc: brackets and angle brackets
/// outside code spans would read as links and HTML, and bare URLs are
/// wrapped to be links.
fn escape(text: &str) -> String {
    static URL: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"https?://[^\s)\]]+[^\s)\].,;:]").expect("regex"));
    let mut out = String::new();
    for (i, part) in text.split('`').enumerate() {
        if i > 0 {
            out.push('`');
        }
        if i % 2 == 1 {
            out.push_str(part); // inside a code span
            continue;
        }
        let mut esc = String::new();
        for c in part.chars() {
            match c {
                '[' | ']' | '<' | '>' | '*' => {
                    esc.push('\\');
                    esc.push(c);
                }
                _ => esc.push(c),
            }
        }
        out.push_str(&URL.replace_all(&esc, "<$0>"));
    }
    out
}

/// The documentation of a field: "channel is the name of ..." becomes "The
/// name of ...", any other opening keeps the field's name, as code.
fn field_doc(f: &Field) -> Vec<String> {
    if f.doc.is_empty() {
        return Vec::new();
    }
    let joined = f.doc.join("\n");
    let mut text = None;
    for verb in [" is ", " are "] {
        if let Some(rest) = joined.strip_prefix(&format!("{}{verb}", f.name)) {
            text = Some(capitalize(rest));
            break;
        }
    }
    let text = text.unwrap_or_else(|| match joined.strip_prefix(&f.name) {
        Some(rest) if !rest.starts_with(|c: char| c.is_alphanumeric() || c == '_') => {
            format!("`{}`{rest}", f.name)
        }
        _ => joined,
    });
    // Escaped as one text, so that a code span may run across lines.
    escape(&text).split('\n').map(str::to_owned).collect()
}

fn capitalize(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        Some(first) => first.to_uppercase().collect::<String>() + c.as_str(),
        None => String::new(),
    }
}

fn message_doc(m: &Message, fallback: &str) -> Vec<String> {
    if m.doc.is_empty() {
        return vec![fallback.to_owned()];
    }
    escape(&m.doc.join("\n"))
        .split('\n')
        .map(str::to_owned)
        .collect()
}

/// Writes a message's struct, and its constructor and setters when it is
/// built by applications. `required` collects its required fields as
/// "Message.field".
#[allow(clippy::too_many_lines)] // the struct, its constructor and setters, in order
fn emit_message(
    w: &mut Writer,
    s: &Schema,
    m: &Message,
    usage: Usage,
    own_constructor: bool,
    doc_fallback: &str,
    required: &mut Reviewed,
) -> Result<(), String> {
    let req: Vec<&Field> = if usage.input {
        m.fields.iter().filter(|f| is_required(f)).collect()
    } else {
        Vec::new()
    };
    for f in &req {
        required.fields.insert(format!("{}.{}", m.name, f.name));
    }
    if req.len() > 1 {
        let names: Vec<&str> = req.iter().map(|f| f.name.as_str()).collect();
        required
            .constructors
            .insert(format!("{}({})", m.name, names.join(", ")));
    }
    w.line("");
    w.doc(&message_doc(m, doc_fallback));
    // Without required fields a message has a default: an output, or an
    // input with nothing to require. With them, `new` is the way to make one.
    let derive_default = req.is_empty();
    w.line(&format!(
        "#[derive(Debug, Clone, Serialize, Deserialize{})]",
        if derive_default { ", Default" } else { "" }
    ));
    w.line("#[non_exhaustive]");
    w.line(&format!("pub struct {} {{", m.name));
    for f in &m.fields {
        let (typ, attr) = field_type(s, f)?;
        let doc = field_doc(f);
        if doc.is_empty() {
            w.line(&format!("/// The `{}` field.", f.name));
        } else {
            w.doc(&doc);
        }
        w.line(attr);
        w.line(&format!("pub {}: {typ},", ident(&f.name)));
    }
    w.line("}");

    if !usage.input || (own_constructor && m.fields.is_empty()) {
        return Ok(());
    }
    w.line("");
    w.line(&format!("impl {} {{", m.name));
    if !own_constructor {
        let mut params = Vec::new();
        let mut generics = Vec::new();
        for (i, f) in req.iter().enumerate() {
            let st = setter(s, f)?;
            // A map's key and value types, named apart for each map.
            let rename = |t: &str| {
                t.replace('K', &format!("K{i}"))
                    .replace('V', &format!("V{i}"))
            };
            if !st.generics.is_empty() {
                let inner = st
                    .generics
                    .strip_prefix('<')
                    .and_then(|g| g.strip_suffix('>'));
                generics.push(rename(inner.expect("generics in angle brackets")));
            }
            let param = if st.generics.is_empty() {
                st.param
            } else {
                st.param
                    .replace("(K, V)", &format!("(K{i}, V{i})"))
                    .replace("(K, ", &format!("(K{i}, "))
            };
            params.push(format!("{}: {param}", ident(&f.name)));
        }
        let generics = if generics.is_empty() {
            String::new()
        } else {
            format!("<{}>", generics.join(", "))
        };
        if req.is_empty() {
            w.line(&format!(
                "/// Returns an empty `{}`: set its fields with the methods below.",
                m.name
            ));
        } else {
            let names: Vec<String> = req.iter().map(|f| format!("`{}`", f.name)).collect();
            w.line(&format!(
                "/// Returns a `{}` with its required {}: {}. Set the others with the methods below.",
                m.name,
                if req.len() == 1 { "field" } else { "fields" },
                names.join(", ")
            ));
        }
        w.line("#[must_use]");
        w.line(&format!(
            "pub fn new{generics}({}) -> Self {{",
            params.join(", ")
        ));
        w.line("Self {");
        for f in &m.fields {
            if req.iter().any(|r| r.name == f.name) {
                let st = setter(s, f)?;
                let value = st.store.replace("value", &ident(&f.name));
                if value == ident(&f.name) {
                    w.line(&format!("{},", ident(&f.name)));
                } else {
                    w.line(&format!("{}: {value},", ident(&f.name)));
                }
            } else {
                w.line(&format!("{}: {},", ident(&f.name), empty_value(s, f)?));
            }
        }
        w.line("}");
        w.line("}");
    }
    for f in &m.fields {
        let st = setter(s, f)?;
        w.line("");
        w.line(&format!("/// Sets `{}`.", f.name));
        w.line("#[must_use]");
        w.line(&format!(
            "pub fn {}{}(mut self, value: {}) -> Self {{",
            ident(&f.name),
            st.generics,
            st.param
        ));
        w.line(&format!("self.{} = {};", ident(&f.name), st.store));
        w.line("self");
        w.line("}");
    }
    w.line("}");
    Ok(())
}

// ---------------------------------------------------------------------------
// The API

/// An API method, as the batch `Command` and `Reply` messages declare it: the
/// field name is the method.
struct ApiCommand {
    /// `presence_stats`, say.
    method: String,
    request: String,
    result: String,
    /// Only Centrifugo PRO has it, its comment in `Command` says.
    pro_only: bool,
    /// The request's comment, for the method's.
    doc: Vec<String>,
}

fn commands(s: &Schema) -> Result<Vec<ApiCommand>, String> {
    let (Some(cmd), Some(reply)) = (s.message("Command"), s.message("Reply")) else {
        return Err("api.proto: Command and Reply messages are required".into());
    };
    let mut out = Vec::new();
    for f in &cmd.fields {
        if OMITTED_COMMANDS.contains(&f.name.as_str()) {
            continue;
        }
        let Some(res) = reply.fields.iter().find(|r| r.name == f.name) else {
            return Err(format!(
                "api.proto: Command.{} has no Reply.{}",
                f.name, f.name
            ));
        };
        let Some(req) = s.message(&f.typ) else {
            return Err(format!(
                "api.proto: Command.{} has no request message",
                f.name
            ));
        };
        out.push(ApiCommand {
            method: f.name.clone(),
            request: f.typ.clone(),
            result: res.typ.clone(),
            pro_only: f.doc.join(" ").contains("Centrifugo PRO only"),
            doc: req.doc.clone(),
        });
    }
    Ok(out)
}

/// The method's summary from its request's comment: "`PublishRequest`
/// publishes data into a channel." becomes "Publishes data into a channel."
fn method_doc(c: &ApiCommand) -> Vec<String> {
    let joined = c.doc.join("\n");
    let text = match joined.strip_prefix(&format!("{} ", c.request)) {
        Some(rest) => capitalize(rest),
        None => format!(
            "Calls the `{}` method of the Centrifugo server API.",
            c.method
        ),
    };
    let mut lines: Vec<String> = escape(&text).split('\n').map(str::to_owned).collect();
    if c.pro_only {
        lines.push(String::new());
        lines.push("Centrifugo PRO only.".into());
    }
    lines
}

#[allow(clippy::too_many_lines)] // one block per generated part, in order
fn generate_api(s: &Schema, required: &mut Reviewed) -> Result<String, String> {
    let cmds = commands(s)?;
    let envelopes: HashSet<&str> = s.methods.iter().map(|m| m.response.as_str()).collect();
    let omitted: HashSet<String> = {
        let mut set = HashSet::new();
        for name in ["Command", "Reply"] {
            for f in &s.message(name).expect("checked").fields {
                if OMITTED_COMMANDS.contains(&f.name.as_str()) {
                    set.insert(f.typ.clone());
                }
            }
        }
        set
    };
    let skip = |name: &str| {
        API_HANDWRITTEN.contains(&name)
            || envelopes.contains(name)
            || omitted.contains(name)
            || wrapper_scalar(s, name).is_some()
    };
    let inputs = closure(s, cmds.iter().map(|c| c.request.as_str()));

    let mut w = Writer { out: String::new() };
    w.line("// Code generated by `cargo xtask generate` from proto/api.proto. DO NOT EDIT.");
    w.line("");
    w.line("use std::collections::BTreeMap;");
    w.line("");
    w.line("use serde::{Deserialize, Serialize};");
    w.line("use serde_json::value::RawValue;");

    for m in &s.messages {
        if skip(&m.name) {
            continue;
        }
        let fallback = format!("The `{}` message of the Centrifugo server API.", m.name);
        emit_message(
            &mut w,
            s,
            m,
            Usage {
                input: inputs.contains(&m.name),
            },
            false,
            &fallback,
            required,
        )?;
    }

    // Every request knows its method, and the rules the client checks.
    for c in &cmds {
        w.line("");
        w.line(&format!(
            "impl crate::request::Sealed for {} {{}}",
            c.request
        ));
        w.line("");
        w.line(&format!("impl crate::Request for {} {{", c.request));
        w.line(&format!("const METHOD: &'static str = {:?};", c.method));
        w.line(&format!("const PRO_ONLY: bool = {};", c.pro_only));
        if VALIDATED.contains(&c.method.as_str()) {
            w.line("");
            w.line("fn validate(&self) -> Result<(), crate::Error> {");
            w.line(&format!("crate::validate::{}(self)", c.method));
            w.line("}");
        }
        w.line("}");
    }

    w.line("");
    w.line("/// Every API method, `batch` included, and whether only Centrifugo PRO has it:");
    w.line("/// for refusing an endpoint ending in a method, and for the hints of errors.");
    w.line("#[cfg(feature = \"client\")]");
    w.line("pub(crate) const METHODS: &[(&str, bool)] = &[");
    w.line("(\"batch\", false),");
    for c in &cmds {
        w.line(&format!("({:?}, {}),", c.method, c.pro_only));
    }
    w.line("];");

    // Commands for async consumers: every method, broadcast included.
    w.line("");
    w.line("impl crate::Command {");
    for c in &cmds {
        w.line("");
        w.line(&format!(
            "/// The `{}` command: what [`Client::{}`](crate::Client::{}) would send, checked",
            c.method,
            ident(&c.method),
            ident(&c.method)
        ));
        w.line("/// as the client checks it, for Centrifugo's async consumers.");
        w.line(&format!(
            "pub fn {}(request: {}) -> Result<Self, crate::Error> {{",
            ident(&c.method),
            c.request
        ));
        w.line("Self::new(request)");
        w.line("}");
    }
    w.line("}");

    // The client, a batch and the blocking client: a method per API method.
    // Broadcast is written by hand: its result is a result per channel.
    w.line("");
    w.line("#[cfg(feature = \"client\")]");
    w.line("impl crate::Client {");
    for c in cmds.iter().filter(|c| c.method != "broadcast") {
        w.line("");
        w.doc(&method_doc(c));
        w.line(&format!(
            "pub async fn {}(&self, request: {}) -> Result<{}, crate::Error> {{",
            ident(&c.method),
            c.request,
            c.result
        ));
        w.line("self.call(request).await");
        w.line("}");
    }
    w.line("}");

    w.line("");
    w.line("#[cfg(feature = \"client\")]");
    w.line("impl crate::Batch {");
    for c in cmds.iter().filter(|c| c.method != "broadcast") {
        w.line("");
        w.line(&format!(
            "/// Adds a `{}` command. Its outcome, once the batch is sent, is what",
            c.method
        ));
        w.line(&format!(
            "/// [`Client::{0}`](crate::Client::{0}) would have returned.",
            ident(&c.method)
        ));
        w.line(&format!(
            "pub fn {}(&mut self, request: {}) -> crate::Pending<{}> {{",
            ident(&c.method),
            c.request,
            c.result
        ));
        w.line("self.add(request)");
        w.line("}");
    }
    w.line("}");

    w.line("");
    w.line("#[cfg(feature = \"blocking\")]");
    w.line("impl crate::blocking::Client {");
    for c in cmds.iter().filter(|c| c.method != "broadcast") {
        w.line("");
        w.doc(&method_doc(c));
        w.line(&format!(
            "pub fn {}(&self, request: {}) -> Result<{}, crate::Error> {{",
            ident(&c.method),
            c.request,
            c.result
        ));
        w.line(&format!(
            "self.run(move |client| async move {{ client.{}(request).await }})",
            ident(&c.method)
        ));
        w.line("}");
    }
    w.line("}");

    w.line("");
    w.line("#[cfg(feature = \"blocking\")]");
    w.line("impl crate::blocking::Batch {");
    for c in cmds.iter().filter(|c| c.method != "broadcast") {
        w.line("");
        w.line(&format!(
            "/// Adds a `{0}` command: see [`Batch::{1}`](crate::Batch::{1}).",
            c.method,
            ident(&c.method)
        ));
        w.line(&format!(
            "pub fn {}(&mut self, request: {}) -> crate::Pending<{}> {{",
            ident(&c.method),
            c.request,
            c.result
        ));
        w.line(&format!("self.inner.{}(request)", ident(&c.method)));
        w.line("}");
    }
    w.line("}");
    Ok(w.out)
}

// ---------------------------------------------------------------------------
// The proxy

fn generate_proxy(s: &Schema) -> Result<String, String> {
    let http: Vec<_> = s.methods.iter().filter(|m| !m.streaming).collect();
    // Every message an HTTP proxy method uses; those of streaming methods,
    // which only GRPC can call, are left out.
    let used = closure(
        s,
        http.iter()
            .flat_map(|m| [m.request.as_str(), m.response.as_str()]),
    );
    // Built by applications: the results they answer with, and what those
    // hold. The requests are what Centrifugo sends.
    let results: Vec<&str> = http
        .iter()
        .flat_map(|m| s.message(&m.response).into_iter())
        .flat_map(|r| r.fields.iter().map(|f| f.typ.as_str()))
        .collect();
    let inputs = closure(s, results);

    let mut w = Writer { out: String::new() };
    w.line("// Code generated by `cargo xtask generate` from proto/proxy.proto. DO NOT EDIT.");
    w.line("");
    w.line("use std::collections::BTreeMap;");
    w.line("");
    w.line("use serde::{Deserialize, Serialize};");
    w.line("use serde_json::value::RawValue;");
    let mut unused = Reviewed::default();
    for m in &s.messages {
        if !used.contains(&m.name) || wrapper_scalar(s, &m.name).is_some() {
            continue;
        }
        let fallback = format!("The `{}` message of the Centrifugo proxy protocol.", m.name);
        let own = PROXY_OWN_CONSTRUCTOR.contains(&m.name.as_str());
        emit_message(
            &mut w,
            s,
            m,
            Usage {
                input: inputs.contains(&m.name),
            },
            own,
            &fallback,
            &mut unused,
        )?;
    }
    if !unused.fields.is_empty() {
        let list: Vec<_> = unused.fields.into_iter().collect();
        return Err(format!(
            "proxy.proto marks fields Required. ({}): decide how the proxy types take them",
            list.join(", ")
        ));
    }
    Ok(w.out)
}

// ---------------------------------------------------------------------------
// The test of every method

/// An expression of a value a setter or constructor takes for `f`, for the
/// test calling every method.
fn dummy(s: &Schema, f: &Field) -> Result<String, String> {
    let one = |typ: &str| -> Result<String, String> {
        Ok(match typ {
            "string" => "\"x\"".into(),
            "bytes" => "raw(\"{}\")".into(),
            "bool" => "true".into(),
            "double" | "float" => "1.0".into(),
            t if SCALARS.contains(&t) => "1".into(),
            t => match wrapper_scalar(s, t) {
                Some("bool") => "true".into(),
                Some(_) => "1".into(),
                None => constructor(s, t)?,
            },
        })
    };
    if f.map_key.is_some() {
        return Ok(format!("[(\"k\", {})]", one(&f.typ)?));
    }
    if f.repeated {
        return Ok(format!("[{}]", one(&f.typ)?));
    }
    one(&f.typ)
}

/// `Message::new(...)` with dummies for its required fields.
fn constructor(s: &Schema, name: &str) -> Result<String, String> {
    let m = s
        .message(name)
        .ok_or_else(|| format!("no message {name}"))?;
    let args: Vec<String> = m
        .fields
        .iter()
        .filter(|f| is_required(f))
        .map(|f| dummy(s, f))
        .collect::<Result<_, _>>()?;
    Ok(format!("{name}::new({})", args.join(", ")))
}

/// A request for a command: its constructor, with data where the client
/// requires it.
fn request_expr(s: &Schema, c: &ApiCommand) -> Result<String, String> {
    let mut e = constructor(s, &c.request)?;
    if VALIDATED.contains(&c.method.as_str()) {
        e.push_str(".data(raw(\"{}\"))");
    }
    Ok(e)
}

#[allow(clippy::too_many_lines)] // one block per test, in order
fn generate_methods_test(s: &Schema) -> Result<String, String> {
    let cmds = commands(s)?;
    let mut w = Writer { out: String::new() };
    w.line("// Code generated by `cargo xtask generate` from proto/api.proto. DO NOT EDIT.");
    w.line("");
    w.line("//! Every API method reaches its endpoint, its batch command and its async");
    w.line("//! consumer command, from the async and the blocking client (spec 15).");
    w.line("");
    w.line("#![cfg(feature = \"client\")]");
    w.line("#![allow(clippy::unwrap_used, clippy::pedantic)] // tests: unwrap is the assertion");
    w.line("");
    w.line("mod common;");
    w.line("");
    w.line("use cent::*;");
    w.line("use common::{Fake, raw};");
    w.line("");
    w.line("const METHODS: &[&str] = &[");
    for c in &cmds {
        w.line(&format!("{:?},", c.method));
    }
    w.line("];");
    w.line("");
    w.line("fn paths(fake: &Fake) -> Vec<String> {");
    w.line("fake.requests().iter().map(|r| r.uri.clone()).collect()");
    w.line("}");
    w.line("");
    w.line("fn endpoints() -> Vec<String> {");
    w.line("METHODS.iter().map(|m| format!(\"http://centrifugo.test/api/{m}\")).collect()");
    w.line("}");
    w.line("");
    w.line("#[tokio::test]");
    w.line("async fn every_method_reaches_its_endpoint() {");
    w.line("let fake = Fake::centrifugo();");
    w.line("let client = Client::new(fake.config()).unwrap();");
    for c in &cmds {
        w.line(&format!(
            "client.{}({}).await.unwrap();",
            ident(&c.method),
            request_expr(s, c)?
        ));
    }
    w.line("assert_eq!(paths(&fake), endpoints());");
    w.line("}");
    w.line("");
    w.line("#[tokio::test]");
    w.line("async fn every_method_has_a_batch_command() {");
    w.line("let fake = Fake::centrifugo();");
    w.line("let client = Client::new(fake.config()).unwrap();");
    w.line("let mut batch = client.new_batch(BatchOptions::default());");
    for (i, c) in cmds.iter().enumerate() {
        w.line(&format!(
            "let p{i} = batch.{}({});",
            ident(&c.method),
            request_expr(s, c)?
        ));
    }
    w.line("batch.send().await.unwrap();");
    for i in 0..cmds.len() {
        w.line(&format!("p{i}.result().unwrap();"));
    }
    w.line("let body = &fake.requests()[0].body;");
    w.line("let keys: Vec<&str> = body[\"commands\"].as_array().unwrap().iter().map(|c| c.as_object().unwrap().keys().next().unwrap().as_str()).collect();");
    w.line("assert_eq!(keys, METHODS);");
    w.line("}");
    w.line("");
    w.line("#[test]");
    w.line("fn every_method_has_a_command() {");
    w.line("let methods = [");
    for c in &cmds {
        w.line(&format!(
            "Command::{}({}).unwrap().method(),",
            ident(&c.method),
            request_expr(s, c)?
        ));
    }
    w.line("];");
    w.line("assert_eq!(methods, METHODS);");
    w.line("}");
    w.line("");
    w.line("#[cfg(feature = \"blocking\")]");
    w.line("#[test]");
    w.line("fn every_method_on_the_blocking_client() {");
    w.line("let fake = Fake::centrifugo();");
    w.line("let client = blocking::Client::new(fake.config()).unwrap();");
    for c in &cmds {
        w.line(&format!(
            "client.{}({}).unwrap();",
            ident(&c.method),
            request_expr(s, c)?
        ));
    }
    w.line("let mut batch = client.new_batch(BatchOptions::default());");
    for (i, c) in cmds.iter().enumerate() {
        w.line(&format!(
            "let p{i} = batch.{}({});",
            ident(&c.method),
            request_expr(s, c)?
        ));
    }
    w.line("batch.send().unwrap();");
    for i in 0..cmds.len() {
        w.line(&format!("p{i}.result().unwrap();"));
    }
    w.line("let mut want = endpoints();");
    w.line("want.push(\"http://centrifugo.test/api/batch\".into());");
    w.line("assert_eq!(paths(&fake), want);");
    w.line("}");
    Ok(w.out)
}
