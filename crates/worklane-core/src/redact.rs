//! Credential redaction for error messages.
//!
//! A backend driver's error can echo the connection string it was given, which
//! may carry a password. Such an error flows into [`Error`](crate::Error), then
//! onward to logs and — when a job fails — into the persisted dead-letter
//! reason. Redacting at the point each driver error becomes an `Error` (the
//! backend error mappers) keeps secrets out of every downstream surface, rather
//! than relying on each call site to remember to scrub.

/// Query-string needles whose values are credentials and must be redacted.
/// Matched case-insensitively. libpq/sqlx accept the password as a query
/// parameter (e.g. `postgres://host/db?password=…`, `?sslpassword=…`), so
/// userinfo redaction alone is not enough. The signed-URL keys cover object-store
/// presigned URLs (the natural backing for a Claim Check `PayloadStore`): a store
/// error that echoes such a URL would otherwise leak the signature/credential.
const CREDENTIAL_QUERY_NEEDLES: [&str; 8] = [
    "password=",
    "sslpassword=",
    "passwd=",
    "signature=",
    "x-amz-credential=",
    "x-amz-security-token=",
    "awsaccesskeyid=",
    "sig=",
];

/// Redact credentials from any connection URL embedded in `msg`.
///
/// Two independent passes, so a secret in either position is removed:
/// 1. **Userinfo** — the `user:pass` between `://` and the authority's `@`.
/// 2. **Query parameters** — the value of any credential query key.
///
/// Text containing no URL (or no credentials) is returned unchanged.
pub fn redact_credentials(msg: &str) -> String {
    redact_query_credentials(&redact_userinfo(msg))
}

/// Redact credential-bearing substrings, then bound the string at a UTF-8
/// character boundary. Appends `...` when truncation occurs.
pub fn redact_and_truncate(msg: &str, max_len: usize) -> String {
    truncate_utf8(redact_credentials(msg), max_len)
}

fn truncate_utf8(mut msg: String, max_len: usize) -> String {
    if msg.len() <= max_len {
        return msg;
    }
    let mut end = max_len;
    while !msg.is_char_boundary(end) {
        end -= 1;
    }
    msg.truncate(end);
    msg.push_str("...");
    msg
}

/// Pass 1: replace the `user:pass@` userinfo of every `scheme://…` authority.
fn redact_userinfo(msg: &str) -> String {
    let mut out = String::with_capacity(msg.len());
    let mut rest = msg;
    while let Some(scheme_pos) = rest.find("://") {
        let after = scheme_pos + 3;
        out.push_str(&rest[..after]);
        let tail = &rest[after..];
        // The URL token ends at the whitespace/quote that terminates it in a log
        // or error line.
        let token_end = tail
            .find([' ', '\t', '\n', '"', '\''])
            .unwrap_or(tail.len());
        let token = &tail[..token_end];
        // Prefer the last `@` so an unencoded `@` inside the password is absorbed
        // too. If the `@` appears only after a path/query marker and the prefix has
        // no `:` user/password shape, treat it as ordinary URL content (e.g.
        // `/path@v2`) rather than destroying the host.
        match token.rfind('@') {
            Some(at) if looks_like_userinfo(&token[..at]) => {
                out.push_str("***@");
                out.push_str(&token[at + 1..]);
            }
            Some(_) => out.push_str(token),
            None => out.push_str(token),
        }
        rest = &tail[token_end..];
    }
    out.push_str(rest);
    out
}

fn looks_like_userinfo(prefix: &str) -> bool {
    prefix.contains(':') || !prefix.contains(['/', '?', '#'])
}

/// Pass 2: replace the value of every credential query key (`key=value`),
/// case-insensitive on the key, with the value running until the next `&` or a
/// delimiter that ends the URL (whitespace/quote).
fn redact_query_credentials(msg: &str) -> String {
    let lower = msg.to_ascii_lowercase();
    let mut out = String::with_capacity(msg.len());
    let mut idx = 0;
    while idx < msg.len() {
        let remaining = &lower[idx..];
        // Find the nearest credential key occurrence in the remaining text, but
        // only where the key starts at a boundary — not as the tail of a longer
        // identifier. `passwd` must match `?passwd=`, `&passwd=`, and the
        // space-separated keyword-DSN form `passwd=secret`, but NOT `notmypasswd=`
        // or `usersig=`. The trailing `=` already anchors the key's end, so only
        // the leading side needs a boundary: the byte before it must not be an
        // identifier char.
        let hit = CREDENTIAL_QUERY_NEEDLES
            .iter()
            .filter_map(|needle| {
                find_key_at_boundary(remaining, needle).map(|pos| (pos, needle.len()))
            })
            .min_by_key(|(pos, _)| *pos);

        let Some((rel_pos, key_len)) = hit else {
            out.push_str(&msg[idx..]);
            break;
        };
        let value_start = idx + rel_pos + key_len;
        out.push_str(&msg[idx..value_start]);
        let value = &msg[value_start..];
        let value_end = value
            .find(['&', ' ', '\t', '\n', '"', '\''])
            .map(|p| value_start + p)
            .unwrap_or(msg.len());
        out.push_str("***");
        idx = value_end;
    }
    out
}

/// The first occurrence of `needle` in `haystack` that starts at a key boundary —
/// at the start, or preceded by a byte that is not an ASCII identifier character
/// (`[A-Za-z0-9_]`). This admits `?`, `&`, and whitespace separators (real
/// credential-key positions) while rejecting a key embedded in a longer
/// identifier (e.g. `passwd` inside `notmypasswd`).
fn find_key_at_boundary(haystack: &str, needle: &str) -> Option<usize> {
    let bytes = haystack.as_bytes();
    let mut from = 0;
    while let Some(rel) = haystack[from..].find(needle) {
        let pos = from + rel;
        let preceded_by_ident =
            pos > 0 && (bytes[pos - 1].is_ascii_alphanumeric() || bytes[pos - 1] == b'_');
        if !preceded_by_ident {
            return Some(pos);
        }
        from = pos + 1;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{redact_and_truncate, redact_credentials};

    #[test]
    fn redacts_userinfo_in_embedded_url() {
        let msg = "connection failed for postgres://alice:s3cret@db.host:5432/jobs now";
        assert_eq!(
            redact_credentials(msg),
            "connection failed for postgres://***@db.host:5432/jobs now"
        );
    }

    #[test]
    fn redacts_password_query_parameter() {
        let msg = "postgres://db.host/jobs?sslmode=require&password=s3cret failed";
        assert_eq!(
            redact_credentials(msg),
            "postgres://db.host/jobs?sslmode=require&password=*** failed"
        );
    }

    #[test]
    fn redacts_both_userinfo_and_query_password() {
        let msg = "redis://user:pw@cache:6379/0?password=other";
        assert_eq!(
            redact_credentials(msg),
            "redis://***@cache:6379/0?password=***"
        );
    }

    #[test]
    fn redacts_userinfo_when_password_contains_slash() {
        // Regression: a `/` in the password must not truncate the authority scan
        // before the `@` (which used to leak the whole `user:pass@host`).
        let msg = "connection failed for postgres://user:p/ss@db.host:5432/jobs now";
        assert_eq!(
            redact_credentials(msg),
            "connection failed for postgres://***@db.host:5432/jobs now"
        );
    }

    #[test]
    fn redacts_userinfo_when_password_contains_question_mark() {
        let msg = "redis://user:p?ss@cache:6379/0 unreachable";
        assert_eq!(
            redact_credentials(msg),
            "redis://***@cache:6379/0 unreachable"
        );
    }

    #[test]
    fn redacts_userinfo_with_slash_and_question_mark_in_password() {
        let msg = "postgres://u:a/b?c=d@host/db failed";
        assert_eq!(redact_credentials(msg), "postgres://***@host/db failed");
    }

    #[test]
    fn redacts_query_key_case_insensitively() {
        let msg = "error ?SSLPassword=Secret123 trailing";
        assert_eq!(redact_credentials(msg), "error ?SSLPassword=*** trailing");
    }

    #[test]
    fn redacts_aws_presigned_url_signature_and_credential() {
        let msg = "payload store read: GET https://b.s3.amazonaws.com/k?\
                   X-Amz-Credential=AKIA/20240101/us-east-1/s3/aws4_request&\
                   X-Amz-Signature=deadbeefcafe&X-Amz-Expires=900 failed";
        let out = redact_credentials(msg);
        assert!(
            !out.contains("AKIA/20240101"),
            "credential must be redacted: {out}"
        );
        assert!(
            !out.contains("deadbeefcafe"),
            "signature must be redacted: {out}"
        );
        assert!(
            out.contains("X-Amz-Expires=900"),
            "non-secret params kept: {out}"
        );
    }

    #[test]
    fn redacts_azure_sas_signature() {
        let msg = "blob error https://acct.blob.core.windows.net/c/b?sv=2021&sig=abc123== denied";
        assert_eq!(
            redact_credentials(msg),
            "blob error https://acct.blob.core.windows.net/c/b?sv=2021&sig=*** denied"
        );
    }

    #[test]
    fn a_credential_key_must_start_at_a_boundary() {
        // A credential key embedded in a longer identifier is NOT a credential and
        // must not be over-redacted.
        assert_eq!(
            redact_credentials("error ?notmypasswd=keep&z=1"),
            "error ?notmypasswd=keep&z=1",
            "`passwd` as the tail of `notmypasswd` is not the credential key"
        );
        assert_eq!(
            redact_credentials("url ?usersig=abc trailing"),
            "url ?usersig=abc trailing",
            "`sig` as the tail of `usersig` is not the credential key"
        );
    }

    #[test]
    fn redacts_keyword_dsn_password() {
        // libpq keyword/value DSN: space-separated, no `?`/`&`. The key is still at
        // a boundary (preceded by a space), so it must be redacted.
        assert_eq!(
            redact_credentials("conn failed: host=db user=app password=s3cret sslmode=require"),
            "conn failed: host=db user=app password=*** sslmode=require"
        );
    }

    #[test]
    fn leaves_url_without_credentials_untouched() {
        let msg = "redis://cache.host:6379 unreachable";
        assert_eq!(redact_credentials(msg), msg);
    }

    #[test]
    fn leaves_path_at_sign_without_userinfo_untouched() {
        let msg = "GET http://example.com/path@v2 failed";
        assert_eq!(redact_credentials(msg), msg);
    }

    #[test]
    fn handles_message_with_no_url() {
        let msg = "timed out after 5s";
        assert_eq!(redact_credentials(msg), msg);
    }

    #[test]
    fn truncates_on_utf8_boundary() {
        let msg = "é".repeat(300);
        let out = redact_and_truncate(&msg, 511);
        assert!(
            out.ends_with("..."),
            "truncated message should carry an ellipsis"
        );
        assert!(out.is_char_boundary(out.len()));
    }
}
