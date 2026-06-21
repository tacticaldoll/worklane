//! Validation for the one user-controlled value the broker interpolates into SQL
//! identifier position: the schema name. Everything else flows through bind
//! parameters; the schema cannot, so it is whitelisted here before it is ever
//! folded into a table name.

/// A schema name proven to be a safe SQL identifier.
///
/// The only constructor is [`SafeSchema::new`], which runs [`is_safe_ident`], so
/// *holding* a `SafeSchema` is the proof that interpolating it into identifier
/// position is safe. This keeps the schema validation invariant in one type that
/// the broker, result store, and query builder can share:
/// a query builder that asks for a `SafeSchema` cannot be handed an unvalidated
/// one. [`qualify`](SafeSchema::qualify) is the single definition of how a table
/// name is schema-qualified, replacing the three open-coded `format!` sites.
#[derive(Debug, Clone)]
pub(crate) struct SafeSchema(String);

impl SafeSchema {
    /// Validate `schema` as a safe identifier, returning `None` if it is not (so
    /// callers can map to their own error variant).
    pub(crate) fn new(schema: &str) -> Option<Self> {
        is_safe_ident(schema).then(|| SafeSchema(schema.to_string()))
    }

    /// The validated schema name.
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }

    /// Qualify `table` as `"schema".table`. The one place the schema is folded
    /// into a table name.
    ///
    /// `table` is `&'static str` by contract: the schema occupies a quoted,
    /// validated identifier position, but `table` is interpolated **unquoted**, so
    /// it must never carry runtime/user input. Requiring a `'static` literal makes
    /// that a compile-time guarantee — a future caller cannot pass a dynamic table
    /// name into the unquoted slot — extending the same proof-carrying discipline
    /// `SafeSchema` already applies to the schema name.
    pub(crate) fn qualify(&self, table: &'static str) -> String {
        format!("\"{}\".{}", self.0, table)
    }
}

/// Whether `s` is a safe SQL identifier (so it can be interpolated as a schema
/// name): a letter or underscore followed by letters, digits, or underscores,
/// and at most 63 bytes. Postgres truncates identifiers to `NAMEDATALEN - 1`
/// (63 bytes by default), so two longer names diverging only past byte 63 would
/// silently collide onto one set of tables — defeating the per-schema isolation.
/// Rejecting over-long names here keeps an accepted schema name distinct in the
/// database. The chars are ASCII-only, so byte length equals character length.
pub(crate) fn is_safe_ident(s: &str) -> bool {
    if s.is_empty() || s.len() > 63 {
        return false;
    }
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::is_safe_ident;

    #[test]
    fn accepts_plain_identifiers() {
        assert!(is_safe_ident("public"));
        assert!(is_safe_ident("_private"));
        assert!(is_safe_ident("wlcfg_123_4"));
        assert!(is_safe_ident(&"a".repeat(63)));
    }

    #[test]
    fn rejects_empty_overlong_and_injection() {
        assert!(!is_safe_ident(""));
        assert!(!is_safe_ident(&"a".repeat(64)));
        assert!(!is_safe_ident("1leading_digit"));
        assert!(!is_safe_ident("has space"));
        assert!(!is_safe_ident("with\"quote"));
        assert!(!is_safe_ident("with.dot"));
        assert!(!is_safe_ident("naïve")); // non-ASCII
    }
}
