use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde_json::Value;

use super::*;

fn at(secs: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(secs)
}

fn claims_of(jwt: &str) -> (Value, Value) {
    let parts: Vec<&str> = jwt.split('.').collect();
    assert_eq!(parts.len(), 3);
    let dec = |p: &str| serde_json::from_slice(&URL_SAFE_NO_PAD.decode(p).unwrap()).unwrap();
    (dec(parts[0]), dec(parts[1]))
}

// Spec 15.1: HS256 tokens, byte for byte.
#[test]
fn vectors() {
    let key = Key::hmac("client_secret").unwrap();
    let c = ConnectionClaims::new("42", Expiry::At(at(1_700_003_600))).issued_at(at(1_700_000_000));
    assert_eq!(
        connection(&key, &c).unwrap(),
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiI0MiIsImV4cCI6MTcwMDAwMzYwMCwiaWF0IjoxNzAwMDAwMDAwfQ.HKZwmcYmHVbRESuYd4swVgAH7gg41giR57F_K5FEmEY"
    );
    let s = SubscriptionClaims::new("42", "news", Expiry::At(at(1_700_003_600)))
        .issued_at(at(1_700_000_000));
    assert_eq!(
        subscription(&key, &s).unwrap(),
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiI0MiIsImV4cCI6MTcwMDAwMzYwMCwiaWF0IjoxNzAwMDAwMDAwLCJjaGFubmVsIjoibmV3cyJ9.v_Quw-azNV1qFjm9ZQnkp6QBtK6LmtFxY74YcUHlJcA"
    );
}

#[test]
fn claims_in_order() {
    let key = Key::hmac("s").unwrap().with_key_id("k1");
    let info = RawValue::from_string(r#"{"name":"Alice"}"#.into()).unwrap();
    let c = ConnectionClaims::new("", Expiry::Never)
        .connection_expiry(Expiry::Never)
        .issued_at(at(1))
        .id("t1")
        .audience("a")
        .issuer("i")
        .info(info)
        .channels(["news"])
        .labels([("region", "eu")])
        .profile("chat");
    let jwt = connection(&key, &c).unwrap();
    let (header, claims) = claims_of(&jwt);
    assert_eq!(
        header,
        serde_json::json!({"alg": "HS256", "typ": "JWT", "kid": "k1"})
    );
    let text = String::from_utf8(
        URL_SAFE_NO_PAD
            .decode(jwt.split('.').nth(1).unwrap())
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        text,
        r#"{"sub":"","iat":1,"jti":"t1","aud":"a","iss":"i","expire_at":0,"info":{"name":"Alice"},"channels":["news"],"labels":{"region":"eu"},"profile":"chat"}"#
    );
    assert!(claims.get("exp").is_none(), "never expiring: no exp");

    // A subscription token's claims, in the order of spec 15.1.
    let s = SubscriptionClaims::new("42", "news", Expiry::At(at(2)))
        .issued_at(at(1))
        .subscription_expiry(Expiry::Never)
        .allow(["pub"]);
    let jwt = subscription(&key, &s).unwrap();
    let text = String::from_utf8(
        URL_SAFE_NO_PAD
            .decode(jwt.split('.').nth(1).unwrap())
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        text,
        r#"{"sub":"42","exp":2,"iat":1,"channel":"news","expire_at":0,"allow":["pub"]}"#
    );
}

#[test]
fn refusals() {
    let key = Key::hmac("s").unwrap();
    assert!(Key::hmac("").is_err());
    let both = ConnectionClaims::new("1", Expiry::Never)
        .channels(["a"])
        .subs([("b", SubscribeOptions::new())]);
    assert!(connection(&key, &both).is_err());
    assert!(subscription(&key, &SubscriptionClaims::new("1", "", Expiry::Never)).is_err());
    assert!(
        connection(
            &key,
            &ConnectionClaims::new("1", Expiry::At(UNIX_EPOCH - Duration::from_secs(1)))
        )
        .is_err()
    );
}

fn generated_keys() -> Vec<Key> {
    use aws_lc_rs::rsa::KeySize;
    use aws_lc_rs::signature::{self, EcdsaKeyPair, Ed25519KeyPair};
    use rustls_pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
    let pkcs8 = |der: Vec<u8>| PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(der));
    let rsa = aws_lc_rs::rsa::KeyPair::generate(KeySize::Rsa2048).unwrap();
    let rsa_der =
        aws_lc_rs::encoding::AsDer::<aws_lc_rs::encoding::Pkcs8V1Der<'_>>::as_der(&rsa).unwrap();
    let mut keys = vec![
        Key::from_der(&pkcs8(rsa_der.as_ref().to_vec()))
            .unwrap()
            .with_key_id("rsa"),
    ];
    for (alg, kid) in [
        (&signature::ECDSA_P256_SHA256_FIXED_SIGNING, "p256"),
        (&signature::ECDSA_P384_SHA384_FIXED_SIGNING, "p384"),
        (&signature::ECDSA_P521_SHA512_FIXED_SIGNING, "p521"),
    ] {
        let doc = EcdsaKeyPair::generate_pkcs8(alg, &aws_lc_rs::rand::SystemRandom::new()).unwrap();
        keys.push(
            Key::from_der(&pkcs8(doc.as_ref().to_vec()))
                .unwrap()
                .with_key_id(kid),
        );
    }
    let doc = Ed25519KeyPair::generate_pkcs8(&aws_lc_rs::rand::SystemRandom::new()).unwrap();
    keys.push(
        Key::from_der(&pkcs8(doc.as_ref().to_vec()))
            .unwrap()
            .with_key_id("ed"),
    );
    keys
}

/// Every asymmetric key signs tokens its published JWK verifies.
#[test]
fn asymmetric_keys_verify_with_their_jwks() {
    use aws_lc_rs::signature::{self, UnparsedPublicKey};
    let keys = generated_keys();
    let set: Value = serde_json::from_str(&jwks(&keys).unwrap()).unwrap();
    let published = set["keys"].as_array().unwrap();
    assert_eq!(published.len(), keys.len());
    let b64 = |v: &Value| URL_SAFE_NO_PAD.decode(v.as_str().unwrap()).unwrap();
    for (key, jwk) in keys.iter().zip(published) {
        assert_eq!(jwk["use"], "sig");
        assert_eq!(jwk["kid"], key.key_id().unwrap());
        assert_eq!(jwk["alg"], key.algorithm());
        let jwt = connection(
            key,
            &ConnectionClaims::new("42", Expiry::after(Duration::from_secs(60))),
        )
        .unwrap();
        let (header, _) = claims_of(&jwt);
        assert_eq!(header["alg"], key.algorithm());
        let (signed, sig) = jwt.rsplit_once('.').unwrap();
        let sig = URL_SAFE_NO_PAD.decode(sig).unwrap();
        match jwk["kty"].as_str().unwrap() {
            "RSA" => {
                let public = aws_lc_rs::rsa::PublicKeyComponents {
                    n: b64(&jwk["n"]),
                    e: b64(&jwk["e"]),
                };
                public
                    .verify(
                        &signature::RSA_PKCS1_2048_8192_SHA256,
                        signed.as_bytes(),
                        &sig,
                    )
                    .unwrap();
            }
            "EC" => {
                let (alg, size): (&dyn signature::VerificationAlgorithm, usize) =
                    match jwk["crv"].as_str().unwrap() {
                        "P-256" => (&signature::ECDSA_P256_SHA256_FIXED, 32),
                        "P-384" => (&signature::ECDSA_P384_SHA384_FIXED, 48),
                        "P-521" => (&signature::ECDSA_P521_SHA512_FIXED, 66),
                        c => panic!("curve {c}"),
                    };
                let (x, y) = (b64(&jwk["x"]), b64(&jwk["y"]));
                assert_eq!(
                    (x.len(), y.len(), sig.len()),
                    (size, size, 2 * size),
                    "fixed sizes"
                );
                let point = [&[4u8][..], &x, &y].concat();
                UnparsedPublicKey::new(alg, point)
                    .verify(signed.as_bytes(), &sig)
                    .unwrap();
            }
            "OKP" => {
                assert_eq!(jwk["crv"], "Ed25519");
                UnparsedPublicKey::new(&signature::ED25519, b64(&jwk["x"]))
                    .verify(signed.as_bytes(), &sig)
                    .unwrap();
            }
            k => panic!("kty {k}"),
        }
    }
}

#[test]
fn jwks_refusals() {
    let keys = generated_keys();
    assert!(jwks(&[]).is_err());
    assert!(jwks(&[Key::hmac("s").unwrap().with_key_id("h")]).is_err());
    assert!(
        jwks(&[
            keys[0].clone().with_key_id("same"),
            keys[1].clone().with_key_id("same")
        ])
        .is_err()
    );
    let mut no_id = keys[4].clone();
    no_id.kid = None;
    assert!(jwks(&[no_id]).is_err());
    assert!(
        jwks(&[keys[4].clone().with_key_id("")]).is_err(),
        "an empty key ID is none"
    );
    let exp = SystemTime::now() + Duration::from_secs(60);
    let claims = ApiClaims::new().audience("a").expires_at(exp);
    assert!(api(&keys[4].clone().with_key_id(""), &claims).is_err());
}

// Test keys refused: RSA of 1024 bits, PKCS#1 and PKCS#8, and ECDSA on
// P-224, SEC1 and PKCS#8. Generated with openssl for these tests only.
const RSA_1024_PKCS1: &str = "-----BEGIN RSA PRIVATE KEY-----\nMIICXQIBAAKBgQDlfgNomMPNbLQwDbAZxIZa1fDFe8s2hNuDVzp+Fr33UveeSXCj\nAYyyHKXEhinm88EzNJGWAmch7dTyNAhxclQpcW0Do+yP7kGAUo9Vq9TKHZs785sj\n0RjUz9CDDUX6IC0f8u38EywA8fL8CoVs+ur3n3P647vnBtItzORgJoIr4QIDAQAB\nAoGBAIK1bh2R/Nuw0UQZV7alb1ynQ7ZHFuwbaad59C/CpjoKwEwgSg4doNQFawju\n3UjUhrKye0U0Yyc4/bNhnXl4A6Rurzi7pGribVE9wUN0L5i2z4gIFMzvpA+uGWIt\nUrTGnvDK2qbVUgOCKH2kAgjSImJqbU4hrj12C35YRhZxfaNBAkEA/mDYAOMsVl92\n8CkikDJqgoYIeMx7MeiWwVlKSoPJTtNfdcaiN2S5zJkUxWomqyO7Bw7BbQNu15zi\nfQXgT49JGQJBAOb0jfAaOPoSatxpcTMxNIopZJ2C7dw6NlG5hOIUzF5oRmRRFgCr\nmVWswMxchpfn2d1J/H5WbdOPmgiBaBseqgkCQQDALJui9/zb990cDIePFnd+yhGG\naTy89LFHutQ0PSGNSoS4rmqInLD9T7zwc4RpEGJQL6aTW9+WRnCY8SKIGICpAkBq\nz20ZPrddB8l9+BHPMBFxWCVdJyEkOZPHhfnrqNy/SJBNcqavVq0KT2O234+rjyFV\n0xK/xu7x6ba9+kMQT8UpAkAcvBHo8fF+11fcnKiI51DlJCW0eqU491v1cmPP5yOX\ncSuu63/cwnaNWYAbwxYopIEBKbo3ocv83qxoUs3smiMV\n-----END RSA PRIVATE KEY-----\n";
const RSA_1024_PKCS8: &str = "-----BEGIN PRIVATE KEY-----\nMIICdwIBADANBgkqhkiG9w0BAQEFAASCAmEwggJdAgEAAoGBAOV+A2iYw81stDAN\nsBnEhlrV8MV7yzaE24NXOn4WvfdS955JcKMBjLIcpcSGKebzwTM0kZYCZyHt1PI0\nCHFyVClxbQOj7I/uQYBSj1Wr1ModmzvzmyPRGNTP0IMNRfogLR/y7fwTLADx8vwK\nhWz66vefc/rju+cG0i3M5GAmgivhAgMBAAECgYEAgrVuHZH827DRRBlXtqVvXKdD\ntkcW7Btpp3n0L8KmOgrATCBKDh2g1AVrCO7dSNSGsrJ7RTRjJzj9s2GdeXgDpG6v\nOLukauJtUT3BQ3QvmLbPiAgUzO+kD64ZYi1StMae8MraptVSA4IofaQCCNIiYmpt\nTiGuPXYLflhGFnF9o0ECQQD+YNgA4yxWX3bwKSKQMmqChgh4zHsx6JbBWUpKg8lO\n0191xqI3ZLnMmRTFaiarI7sHDsFtA27XnOJ9BeBPj0kZAkEA5vSN8Bo4+hJq3Glx\nMzE0iilknYLt3Do2UbmE4hTMXmhGZFEWAKuZVazAzFyGl+fZ3Un8flZt04+aCIFo\nGx6qCQJBAMAsm6L3/Nv33RwMh48Wd37KEYZpPLz0sUe61DQ9IY1KhLiuaoicsP1P\nvPBzhGkQYlAvppNb35ZGcJjxIogYgKkCQGrPbRk+t10HyX34Ec8wEXFYJV0nISQ5\nk8eF+euo3L9IkE1ypq9WrQpPY7bfj6uPIVXTEr/G7vHptr36QxBPxSkCQBy8Eejx\n8X7XV9ycqIjnUOUkJbR6pTj3W/VyY8/nI5dxK67rf9zCdo1ZgBvDFiikgQEpujeh\ny/zerGhSzeyaIxU=\n-----END PRIVATE KEY-----\n";
const P224_SEC1: &str = "-----BEGIN EC PRIVATE KEY-----\nMGgCAQEEHMsWHrcjAZ7hB9hD7FaiqvJuqQj0/nFchXtCNfWgBwYFK4EEACGhPAM6\nAATd1zPlr2KMYthx8lio2rV4OI8GStH8dcWbuAYI9zYfGf9LBZVtX1S6SrGVZBWl\nybPWXvQDALGTTA==\n-----END EC PRIVATE KEY-----\n";
const P224_PKCS8: &str = "-----BEGIN PRIVATE KEY-----\nMHgCAQAwEAYHKoZIzj0CAQYFK4EEACEEYTBfAgEBBBzLFh63IwGe4QfYQ+xWoqry\nbqkI9P5xXIV7QjX1oTwDOgAE3dcz5a9ijGLYcfJYqNq1eDiPBkrR/HXFm7gGCPc2\nHxn/SwWVbV9UukqxlWQVpcmz1l70AwCxk0w=\n-----END PRIVATE KEY-----\n";

/// Spec 11.1: an RSA key under 2048 bits is refused as an ECDSA key on another
/// curve is, at the same point - building the `Key` - and with the same error.
/// No `Key` exists for either, so neither signs a connection, subscription or
/// API token, nor is published in a JWKS.
#[test]
fn small_rsa_refused_as_another_curve() {
    let curve = Key::from_pem(P224_PKCS8).unwrap_err();
    assert_eq!(
        Key::from_pem(P224_SEC1).unwrap_err().to_string(),
        curve.to_string()
    );
    for pem in [RSA_1024_PKCS1, RSA_1024_PKCS8] {
        let err = Key::from_pem(pem).unwrap_err();
        assert_eq!(err.to_string(), curve.to_string());
        assert!(err.to_string().contains("RSA of 2048"), "{err}");
    }
}

#[test]
fn pem_keys() {
    let pem = "-----BEGIN PRIVATE KEY-----\nMC4CAQAwBQYDK2VwBCIEIAICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIC\n-----END PRIVATE KEY-----\n";
    let key = Key::from_pem(pem).unwrap();
    assert_eq!(key.algorithm(), "EdDSA");
    // The same key as from its seed, of 32 bytes of 2: the one the
    // integration tests' Centrifugo config verifies.
    let seeded = Key::ed25519(&[2; 32]).unwrap();
    let c = ConnectionClaims::new("1", Expiry::Never).issued_at(at(1));
    assert_eq!(
        connection(&key, &c).unwrap(),
        connection(&seeded, &c).unwrap()
    );
    assert!(Key::from_pem("not a key").is_err());
}

#[test]
fn api_token_source() {
    let keys = generated_keys();
    let ed = keys[4].clone();
    let claims = ApiClaims::new()
        .audience("centrifugo-api")
        .scope(["centrifugo:api", "x"])
        .subject("svc");
    let tokens = ApiTokenSource::new(ed.clone(), claims.clone());
    let t1 = tokens.token().unwrap();
    assert_eq!(t1, tokens.token().unwrap(), "reused for half its ttl");
    let (header, c) = claims_of(&t1);
    assert_eq!(header["kid"], "ed");
    assert_eq!(c["scope"], "centrifugo:api x");
    assert_eq!(c["aud"], "centrifugo-api");
    assert_eq!(c["exp"].as_i64().unwrap() - c["iat"].as_i64().unwrap(), 300);

    let short = ApiTokenSource::new(ed.clone(), claims.clone()).ttl(Duration::from_millis(1));
    let s1 = short.token().unwrap();
    std::thread::sleep(Duration::from_millis(1100));
    assert_ne!(s1, short.token().unwrap(), "refreshed past half its ttl");

    let exp = SystemTime::now() + Duration::from_secs(60);
    assert!(
        api(
            &Key::hmac("s").unwrap().with_key_id("h"),
            &claims.clone().expires_at(exp)
        )
        .is_err()
    );
    let mut no_id = ed.clone();
    no_id.kid = None;
    assert!(api(&no_id, &claims.clone().expires_at(exp)).is_err());
    assert!(
        api(&ed, &ApiClaims::new().subject("x").expires_at(exp)).is_err(),
        "no audience or scope"
    );
    assert!(api(&ed, &claims).is_err(), "no expiry");
    assert!(api(&ed, &ApiClaims::new().scope(["a b"]).expires_at(exp)).is_err());
    assert!(api(&ed, &ApiClaims::new().scope([""]).expires_at(exp)).is_err());
    assert!(
        ApiTokenSource::new(ed.clone(), claims.clone().expires_at(exp))
            .token()
            .is_err()
    );
    assert!(
        ApiTokenSource::new(ed, claims)
            .ttl(Duration::ZERO)
            .token()
            .is_err()
    );
}
