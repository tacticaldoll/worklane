//! Typed background jobs for Rust services.
//!
//! `worklane` is the public-facing facade crate. At the project baseline it is
//! an empty placeholder; the typed `Job`, `Client`, and `Worker` APIs are
//! defined in `openspec/specs/` and implemented in the first OpenSpec change.
//!
//! Core loop: typed payload -> envelope -> broker reserve -> dispatch by kind
//! -> run handler -> ack / retry / fail / dead-letter.
