use anyhow::{Result as AnyResult, anyhow, bail};
use async_nats::jetstream::{self, consumer as nats};
use aws_lc_rs::signature::Ed25519KeyPair;
use feldera_types::transport::nats as cfg;
use std::{sync::Arc, time::Duration};

/// Decodes a NATS seed ("SU..." for users) to the raw 32-byte Ed25519 seed.
///
/// The container format — RFC 4648 base32 without padding over two prefix
/// bytes, the 32-byte seed, and a trailing little-endian CRC-16/XMODEM — is
/// data framing, not cryptography; decoding it here lets the Ed25519 signing
/// itself go through aws-lc-rs.
fn decode_seed(seed: &str) -> AnyResult<[u8; 32]> {
    let raw = data_encoding::BASE32_NOPAD
        .decode(seed.as_bytes())
        .map_err(|e| anyhow!("invalid NATS nkey seed: {e}"))?;
    // Two prefix bytes + 32-byte seed + two CRC bytes.
    if raw.len() != 36 {
        bail!(
            "invalid NATS nkey seed: decoded to {} bytes, expected 36",
            raw.len()
        );
    }
    let (payload, crc) = raw.split_at(raw.len() - 2);
    if crc16_xmodem(payload) != u16::from_le_bytes([crc[0], crc[1]]) {
        bail!("invalid NATS nkey seed: checksum mismatch");
    }
    // The seed prefix (18 << 3) occupies the top five bits of the first
    // byte; the remaining bits carry the public-key prefix, which does not
    // matter for signing.
    if payload[0] & 0xF8 != 0x90 {
        bail!("invalid NATS nkey seed: not a seed");
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&payload[2..34]);
    Ok(out)
}

fn crc16_xmodem(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &byte in data {
        crc ^= (byte as u16) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
        }
    }
    crc
}

pub async fn translate_connect_options(
    config: &cfg::ConnectOptions,
) -> AnyResult<async_nats::ConnectOptions> {
    let connection_timeout = config.connection_timeout().into();
    let request_timeout = config.request_timeout().into();

    let mut options = async_nats::ConnectOptions::new()
        .connection_timeout(connection_timeout)
        .request_timeout(Some(request_timeout));

    options = apply_auth(options, &config.auth).await?;

    if let Some(path) = config.tls.root_certificates_file.as_ref() {
        options = options.add_root_certificates(path.clone());
    }
    if config.tls.require_tls {
        options = options.require_tls(true);
    }

    Ok(options)
}

/// Applies the configured authentication method to the connect options.
///
/// Exactly one method may be configured: `credentials`, `jwt` (which
/// requires `nkey` for nonce signing), bare `nkey`, `token`, or
/// `user_and_password`. Configuring none is valid (unauthenticated
/// connection); configuring more than one is rejected rather than silently
/// picking a winner.
async fn apply_auth(
    options: async_nats::ConnectOptions,
    auth: &cfg::Auth,
) -> AnyResult<async_nats::ConnectOptions> {
    validate_auth(auth)?;

    if let Some(creds) = auth.credentials.as_ref() {
        return Ok(match creds {
            cfg::Credentials::FromFile(path) => options.credentials_file(path).await?,
            cfg::Credentials::FromString(c) => options.credentials(c)?,
        });
    }

    if let Some(jwt) = auth.jwt.as_ref() {
        let Some(seed) = auth.nkey.as_ref() else {
            bail!(
                "NATS `jwt` authentication requires `nkey` (the seed that signs the connection nonce)"
            );
        };
        // The Ed25519 signing goes through aws-lc-rs so that FIPS builds
        // (`--features fips`) execute only AWS-LC primitives on this path
        // (EdDSA is FIPS 186-5, included in the AWS-LC FIPS module).
        let seed = decode_seed(seed)?;
        let key_pair = Arc::new(
            Ed25519KeyPair::from_seed_unchecked(&seed)
                .map_err(|e| anyhow!("invalid NATS nkey seed: {e}"))?,
        );
        return Ok(options.jwt(jwt.clone(), move |nonce| {
            let key_pair = key_pair.clone();
            async move { Ok(key_pair.sign(&nonce).as_ref().to_vec()) }
        }));
    }

    if let Some(seed) = auth.nkey.as_ref() {
        // Validated here for a precise error; the nonce signing itself
        // happens inside async-nats (which uses the nkeys crate).
        decode_seed(seed)?;
        return Ok(options.nkey(seed.clone()));
    }

    if let Some(token) = auth.token.as_ref() {
        return Ok(options.token(token.clone()));
    }

    if let Some(cfg::UserAndPassword { user, password }) = auth.user_and_password.as_ref() {
        return Ok(options.user_and_password(user.clone(), password.clone()));
    }

    Ok(options)
}

/// How long the server keeps a consumer that nobody pulls from.
///
/// The connector deletes its consumers itself; this only bounds how long one
/// outlives a process that died without cleaning up. A reading connector
/// always has a pull request outstanding, so it never comes close.
pub const CONSUMER_INACTIVE_THRESHOLD: Duration = Duration::from_secs(60);

/// Builds the configuration of the consumers the connector reads with.
///
/// Every consumer is an ephemeral, single-replica, in-memory pull consumer
/// without acknowledgments, like the client library's ordered consumer. The
/// connector tracks the stream position itself and recreates the consumer at
/// that position when it detects a gap, instead of letting the library do it
/// behind its back (see the module documentation of `input`). The deliver
/// policy is the configured one; the caller overrides it when it resumes.
pub fn translate_consumer_options(config: &cfg::ConsumerConfig) -> nats::pull::Config {
    nats::pull::Config {
        name: config.name.clone(),
        description: config.description.clone(),
        filter_subjects: config.filter_subjects.clone(),
        replay_policy: translate_replay_policy(&config.replay_policy),
        rate_limit: config.rate_limit,
        deliver_policy: translate_deliver_policy(&config.deliver_policy),
        ack_policy: nats::AckPolicy::None,
        max_waiting: config.max_waiting,
        metadata: config.metadata.clone(),
        max_batch: config.max_batch.unwrap_or_default(),
        max_bytes: config.max_bytes.unwrap_or_default(),
        max_expires: config.max_expiry.map(Into::into).unwrap_or_default(),
        inactive_threshold: CONSUMER_INACTIVE_THRESHOLD,
        num_replicas: 1,
        memory_storage: true,
        ..Default::default()
    }
}

/// Checks the parts of the configuration that no server can fix, so that a
/// mistake fails the connector when it opens instead of retrying forever.
pub fn validate_input_config(config: &cfg::NatsInputConfig) -> AnyResult<()> {
    validate_stream_name(&config.stream_name)?;
    for filter in &config.consumer_config.filter_subjects {
        validate_subject(filter).map_err(|error| {
            anyhow!("Invalid NATS input configuration: filter subject {filter:?} {error}")
        })?;
    }
    if matches!(
        config.consumer_config.deliver_policy,
        cfg::DeliverPolicy::LastPerSubject
    ) && config.consumer_config.filter_subjects.is_empty()
    {
        bail!(
            "Invalid NATS input configuration: `deliver_policy` `LastPerSubject` needs `filter_subjects`, because NATS delivers the last message per subject only for a consumer whose filter subject is set; add `filter_subjects`, for example `[\">\"]` for every subject"
        );
    }
    if config.consumer_config.rate_limit != 0 {
        bail!(
            "Invalid NATS input configuration: `rate_limit` is {}, but the connector reads with a pull consumer and NATS supports a rate limit only on push consumers; remove `rate_limit`",
            config.consumer_config.rate_limit
        );
    }
    validate_auth(&config.connection_config.auth)?;
    Ok(())
}

/// JetStream stream names may not be empty and may not contain whitespace,
/// `.`, `*`, `>`, `/` or `\`.
fn validate_stream_name(name: &str) -> AnyResult<()> {
    if name.is_empty() {
        bail!("Invalid NATS input configuration: `stream_name` is empty");
    }
    if let Some(c) = name
        .chars()
        .find(|c| c.is_whitespace() || matches!(c, '.' | '*' | '>' | '/' | '\\'))
    {
        bail!(
            "Invalid NATS input configuration: `stream_name` {name:?} contains {c:?}, which JetStream does not allow in a stream name"
        );
    }
    Ok(())
}

/// A subject filter is a non-empty sequence of non-empty `.`-separated tokens
/// without whitespace, where `*` and `>` are only whole tokens and `>` only
/// the last one.
fn validate_subject(subject: &str) -> Result<(), &'static str> {
    if subject.is_empty() {
        return Err("is empty");
    }
    if subject.chars().any(char::is_whitespace) {
        return Err("contains whitespace");
    }
    let tokens: Vec<&str> = subject.split('.').collect();
    for (i, token) in tokens.iter().enumerate() {
        if token.is_empty() {
            return Err("has an empty token");
        }
        if token.len() > 1 && (token.contains('*') || token.contains('>')) {
            return Err("uses a wildcard inside a token");
        }
        if *token == ">" && i + 1 != tokens.len() {
            return Err("uses `>` before the last token");
        }
    }
    Ok(())
}

/// The authentication checks of [apply_auth] that need no I/O.
fn validate_auth(auth: &cfg::Auth) -> AnyResult<()> {
    let configured = [
        auth.credentials.is_some(),
        auth.jwt.is_some(),
        // A bare nkey is its own method; alongside jwt it is part of the
        // jwt method (the seed signs the connection nonce).
        auth.nkey.is_some() && auth.jwt.is_none(),
        auth.token.is_some(),
        auth.user_and_password.is_some(),
    ]
    .iter()
    .filter(|&&set| set)
    .count();
    if configured > 1 {
        bail!(
            "multiple NATS authentication methods configured; set exactly one of `credentials`, `jwt` (with `nkey`), `nkey`, `token`, or `user_and_password`"
        );
    }
    if auth.jwt.is_some() && auth.nkey.is_none() {
        bail!(
            "NATS `jwt` authentication requires `nkey` (the seed that signs the connection nonce)"
        );
    }
    if let Some(seed) = auth.nkey.as_ref() {
        decode_seed(seed)?;
    }
    Ok(())
}

/// Hides the user information (`user:password@` or `token@`) of every URL in
/// a NATS server URL list, for logs and error messages.
pub fn redact_server_url(url: &str) -> String {
    url.split(',')
        .map(|url| {
            let (scheme, rest) = match url.split_once("://") {
                Some((scheme, rest)) => (Some(scheme), rest),
                None => (None, url),
            };
            let authority_end = rest.find('/').unwrap_or(rest.len());
            let rest = match rest[..authority_end].rfind('@') {
                Some(at) => format!("***@{}", &rest[at + 1..]),
                None => rest.to_string(),
            };
            match scheme {
                Some(scheme) => format!("{scheme}://{rest}"),
                None => rest,
            }
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// Whether a JetStream API error is permanent: the server rejected the
/// request itself, so it will reject it again however often it is repeated.
///
/// JetStream reports a malformed or inconsistent request (an invalid
/// consumer configuration, a filter that does not match the stream, ...) as
/// a 400. Exhausted resources are 400s too, but those clear once other
/// consumers go away, and a name collision clears with the next attempt's
/// fresh name; they and every other status (404 for a stream that does not
/// exist yet, 5xx for an unavailable cluster) are worth retrying.
pub fn is_permanent_api_error(error: &jetstream::Error) -> bool {
    use jetstream::ErrorCode;
    const RETRYABLE: [ErrorCode; 8] = [
        ErrorCode::ACCOUNT_RESOURCES_EXCEEDED,
        ErrorCode::INSUFFICIENT_RESOURCES,
        ErrorCode::MAXIMUM_CONSUMERS_LIMIT,
        ErrorCode::MEMORY_RESOURCES_EXCEEDED,
        ErrorCode::STORAGE_RESOURCES_EXCEEDED,
        ErrorCode::CONSUMER_NAME_EXIST,
        ErrorCode::CONSUMER_ALREADY_EXISTS,
        ErrorCode::CONSUMER_EXISTING_ACTIVE,
    ];
    error.code() == 400 && !RETRYABLE.contains(&error.error_code())
}

fn translate_replay_policy(p: &cfg::ReplayPolicy) -> nats::ReplayPolicy {
    match p {
        cfg::ReplayPolicy::Instant => nats::ReplayPolicy::Instant,
        cfg::ReplayPolicy::Original => nats::ReplayPolicy::Original,
    }
}

fn translate_deliver_policy(p: &cfg::DeliverPolicy) -> nats::DeliverPolicy {
    match p {
        cfg::DeliverPolicy::All => nats::DeliverPolicy::All,
        cfg::DeliverPolicy::Last => nats::DeliverPolicy::Last,
        cfg::DeliverPolicy::New => nats::DeliverPolicy::New,
        cfg::DeliverPolicy::ByStartSequence { start_sequence } => {
            nats::DeliverPolicy::ByStartSequence {
                start_sequence: *start_sequence,
            }
        }
        cfg::DeliverPolicy::ByStartTime { start_time } => nats::DeliverPolicy::ByStartTime {
            start_time: *start_time,
        },
        cfg::DeliverPolicy::LastPerSubject => nats::DeliverPolicy::LastPerSubject,
    }
}

#[cfg(test)]
mod test {
    use super::decode_seed;

    #[test]
    fn decode_seed_roundtrip() {
        // A seed framing the 32-byte sequence 00..1f, generated with the
        // reference nkeys encoding (user public prefix).
        let seed = "SUAAAAICAMCAKBQHBAEQUCYMBUHA6EARCIJRIFIWC4MBSGQ3DQOR4H776Y";
        let raw = decode_seed(seed).unwrap();
        assert_eq!(raw, core::array::from_fn(|i| i as u8));
        // Agrees with the nkeys crate's own decoding: same seed, same key.
        let ours = aws_lc_rs::signature::Ed25519KeyPair::from_seed_unchecked(&raw).unwrap();
        let theirs = nkeys::KeyPair::from_seed(seed).unwrap();
        let msg = b"nonce";
        theirs
            .verify(msg, ours.sign(msg).as_ref())
            .expect("aws-lc-rs signature must verify under the nkeys-derived public key");
    }

    #[test]
    fn decode_seed_rejects() {
        assert!(decode_seed("not base32!").is_err());
        assert!(decode_seed("SUAAAAICAMCAKBQHBAEQUCYMBUHA6EARCIJRIFIWC4MBSGQ3DQOR4H777Y").is_err()); // bad crc
        // A public key (prefix 'U', not a seed) must be rejected.
        let public = nkeys::KeyPair::new_user().public_key();
        assert!(decode_seed(&public).is_err());
    }
}
