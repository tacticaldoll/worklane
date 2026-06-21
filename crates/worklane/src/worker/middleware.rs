//! Onion-style middleware around job dispatch.
//!
//! A [`Middleware`] wraps the handler: it runs code before and after the inner
//! call, can short-circuit (return without invoking the handler), and can inspect
//! or transform the handler's result. It is the place for cross-cutting concerns —
//! structured logging, metrics, tracing spans, per-call setup/teardown — written
//! once instead of in every handler.
//!
//! Middleware run in registration order, **outermost first**: the first one added
//! via [`Worker::with_middleware`](crate::Worker::with_middleware) sees the call go
//! in first and the result come back last, wrapping all the others and the handler.
//!
//! ```no_run
//! use std::time::Instant;
//! use worklane::{JobContext, Middleware, Next, Result, async_trait};
//!
//! struct LogTiming;
//!
//! #[async_trait]
//! impl Middleware for LogTiming {
//!     async fn handle(&self, ctx: JobContext, payload: &[u8], next: Next<'_>) -> Result<Vec<u8>> {
//!         let kind = ctx.kind.clone();
//!         let started = Instant::now();
//!         let result = next.run(ctx, payload).await; // call the rest of the chain
//!         tracing::info!(kind = %kind, elapsed = ?started.elapsed(), ok = result.is_ok(), "handled");
//!         result
//!     }
//! }
//! ```

use std::sync::Arc;

use async_trait::async_trait;
use worklane_core::{JobContext, Result};

use super::Dispatch;

/// A handler-dispatch interceptor. See [`Worker::with_middleware`](crate::Worker::with_middleware)
/// for registration and ordering.
///
/// Implementations must call `next.run(ctx, payload).await` to invoke the rest of
/// the chain (the next middleware, ending at the handler), unless they deliberately
/// short-circuit. Returning `Err` (or a short-circuit `Err`) flows through the
/// worker's normal failure path (retry / dead-letter), exactly as a handler error.
#[async_trait]
pub trait Middleware: Send + Sync {
    /// Wrap one dispatch. Call `next.run(ctx, payload)` to continue the chain.
    async fn handle(&self, ctx: JobContext, payload: &[u8], next: Next<'_>) -> Result<Vec<u8>>;
}

/// The continuation handed to a [`Middleware`]: the remaining middleware chain,
/// terminating at the job's handler. Call [`run`](Next::run) to proceed.
pub struct Next<'a> {
    pub(super) chain: &'a [Arc<dyn Middleware>],
    pub(super) handler: &'a dyn Dispatch,
}

impl<'a> Next<'a> {
    /// Build the continuation over `chain` (run in order) terminating at `handler`.
    pub(super) fn new(chain: &'a [Arc<dyn Middleware>], handler: &'a dyn Dispatch) -> Self {
        Next { chain, handler }
    }

    /// Invoke the next layer: the next middleware if any remain, otherwise the
    /// handler itself.
    pub async fn run(self, ctx: JobContext, payload: &[u8]) -> Result<Vec<u8>> {
        match self.chain.split_first() {
            Some((mw, rest)) => mw.handle(ctx, payload, Next::new(rest, self.handler)).await,
            None => self.handler.dispatch(ctx, payload).await,
        }
    }
}
