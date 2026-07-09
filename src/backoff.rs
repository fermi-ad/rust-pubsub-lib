//! Shared exponential-backoff helpers for broker reconnect loops.
//!
//! Both the Redis and Kafka backends use the same backoff-and-log pattern:
//! sleep for the current wait duration, double it (capped at [`MAX_BACKOFF`]), and suppress
//! repeated log lines for the same error kind until the error changes or the connection recovers.
//!
//! Two error-reporting methods are provided:
//!
//! - [`OutageState::on_error`] — logs (deduplicated), sleeps for the current backoff duration, and
//!   advances the timer. Use this in **reconnect loops** where sleeping between attempts is correct.
//! - [`OutageState::record_error`] — logs (deduplicated) and updates state, but **does not sleep**.
//!   Use this inside message-processing loops where blocking the stream would be wrong.

use std::fmt::Debug as DebugFmt;

use tokio::time::{Duration, sleep};
use tracing::error;

/// Maximum exponential-backoff delay applied after connection failures.
pub(crate) const MAX_BACKOFF: Duration = Duration::from_secs(30);

const INITIAL_BACKOFF: Duration = Duration::from_secs(1);

/// Tracks the error-deduplication and backoff state for a single reconnect loop.
///
/// `K` is the "error kind" key used for deduplication — typically `redis::ErrorKind` for Redis
/// backends or `std::mem::Discriminant<KafkaError>` for the Kafka backend. Any `PartialEq`
/// type works.
///
/// Call [`OutageState::on_error`] when an error occurs and [`OutageState::on_recovery`] when the
/// connection is restored. The state suppresses repeated log lines for the same `K` value and
/// resets the backoff timer on recovery.
pub(crate) struct OutageState<K: PartialEq> {
    last_error_kind: Option<K>,
    next_backoff: Duration,
}

impl<K: PartialEq> Default for OutageState<K> {
    fn default() -> Self {
        Self {
            last_error_kind: None,
            next_backoff: INITIAL_BACKOFF,
        }
    }
}

impl<K: PartialEq> OutageState<K> {
    /// Returns `true` if an outage is currently in progress (i.e. at least one error has been
    /// seen since the last recovery).
    pub(crate) fn is_in_outage(&self) -> bool {
        self.last_error_kind.is_some()
    }

    /// Logs the error (deduplicated by `K`) and updates outage state, but **does not sleep**.
    ///
    /// Use this inside message-processing loops (e.g. a running stream) where sleeping would
    /// stall delivery of subsequent messages. Backoff is the caller's responsibility.
    ///
    /// Returns `true` if this was a new error kind (i.e. the log line was emitted), `false` if
    /// the same kind was already recorded and the log line was suppressed.
    ///
    /// `kind` is the comparable key for this error (e.g. `err.kind()` for Redis, or
    /// `mem::discriminant(&err)` for Kafka).
    pub(crate) fn record_error(
        &mut self,
        kind: K,
        err: &(dyn DebugFmt + Send + Sync),
        context: &str,
    ) -> bool {
        if Some(&kind) != self.last_error_kind.as_ref() {
            error!("{context}: {err:?}");
            self.last_error_kind = Some(kind);
            true
        } else {
            false
        }
    }

    /// Logs the error (deduplicated by `K`), sleeps for the current backoff duration, and
    /// advances the backoff timer.
    ///
    /// Use this in **reconnect loops** where sleeping between failed attempts is correct.
    ///
    /// `kind` is the comparable key for this error (e.g. `err.kind()` for Redis, or
    /// `mem::discriminant(&err)` for Kafka).
    pub(crate) async fn on_error(
        &mut self,
        kind: K,
        err: &(dyn DebugFmt + Send + Sync),
        context: &str,
    ) {
        self.record_error(kind, err, context);
        // Always sleep, even on a duplicated error type
        sleep(self.next_backoff).await;
        self.next_backoff = (self.next_backoff * 2).min(MAX_BACKOFF);
    }

    /// Resets the outage state after a successful connection.
    ///
    /// Returns `true` if the state was previously in an outage (so the caller can log a recovery
    /// message if desired).
    pub(crate) fn on_recovery(&mut self) -> bool {
        let was_in_outage = self.is_in_outage();
        self.last_error_kind = None;
        self.next_backoff = INITIAL_BACKOFF;
        was_in_outage
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Use a simple integer as the error kind key for tests — no external crate needed.
    type TestState = OutageState<u8>;

    fn make_state() -> TestState {
        OutageState::default()
    }

    #[derive(Debug)]
    struct FakeErr;

    async fn on_err(state: &mut TestState, kind: u8) {
        state.on_error(kind, &FakeErr, "ctx").await;
    }

    #[tokio::test(start_paused = true)]
    async fn on_recovery_returns_false_when_no_outage() {
        let mut state = make_state();
        assert!(!state.on_recovery(), "no outage yet — should return false");
    }

    #[tokio::test(start_paused = true)]
    async fn on_recovery_returns_true_after_error_then_false_again() {
        let mut state = make_state();
        on_err(&mut state, 1).await;
        assert!(
            state.on_recovery(),
            "outage was active — should return true"
        );
        assert!(
            !state.on_recovery(),
            "already recovered — should return false"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn is_in_outage_tracks_error_and_recovery() {
        let mut state = make_state();
        assert!(!state.is_in_outage());
        on_err(&mut state, 1).await;
        assert!(state.is_in_outage());
        state.on_recovery();
        assert!(!state.is_in_outage());
    }

    #[tokio::test(start_paused = true)]
    async fn backoff_starts_at_one_second_and_doubles() {
        let mut state = make_state();

        // First call: sleeps 1 s, next backoff = 2 s
        on_err(&mut state, 1).await;
        assert_eq!(state.next_backoff, Duration::from_secs(2));

        // Second call: sleeps 2 s, next backoff = 4 s
        on_err(&mut state, 1).await;
        assert_eq!(state.next_backoff, Duration::from_secs(4));
    }

    #[tokio::test(start_paused = true)]
    async fn backoff_caps_at_max() {
        let mut state = make_state();

        // After 6 calls the stored next-backoff is capped to 30 s
        for _ in 0..6 {
            on_err(&mut state, 1).await;
        }
        assert_eq!(state.next_backoff, MAX_BACKOFF);

        // Further calls stay capped
        on_err(&mut state, 1).await;
        assert_eq!(state.next_backoff, MAX_BACKOFF);
    }

    #[tokio::test(start_paused = true)]
    async fn backoff_resets_to_one_second_after_recovery() {
        let mut state = make_state();

        on_err(&mut state, 1).await; // backoff_time → 2 s
        assert_eq!(state.next_backoff, Duration::from_secs(2));

        state.on_recovery();
        assert_eq!(
            state.next_backoff, INITIAL_BACKOFF,
            "backoff should be cleared on recovery"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn same_error_kind_is_not_logged_twice() {
        let mut state = make_state();
        on_err(&mut state, 1).await;
        assert_eq!(state.last_error_kind, Some(1));

        on_err(&mut state, 1).await;
        assert_eq!(state.last_error_kind, Some(1));
    }

    #[tokio::test(start_paused = true)]
    async fn different_error_kind_updates_last_error_kind() {
        let mut state = make_state();
        on_err(&mut state, 1).await;
        assert_eq!(state.last_error_kind, Some(1));

        on_err(&mut state, 2).await;
        assert_eq!(state.last_error_kind, Some(2));
    }

    // --- record_error (non-sleeping variant) ---

    #[test]
    fn record_error_returns_true_for_new_kind() {
        let mut state = make_state();
        assert!(
            state.record_error(1, &FakeErr, "ctx"),
            "first occurrence of kind 1 should return true"
        );
    }

    #[test]
    fn record_error_returns_false_for_same_kind() {
        let mut state = make_state();
        state.record_error(1, &FakeErr, "ctx");
        assert!(
            !state.record_error(1, &FakeErr, "ctx"),
            "repeated kind 1 should return false (suppressed)"
        );
    }

    #[test]
    fn record_error_returns_true_for_different_kind() {
        let mut state = make_state();
        state.record_error(1, &FakeErr, "ctx");
        assert!(
            state.record_error(2, &FakeErr, "ctx"),
            "new kind 2 should return true"
        );
        assert_eq!(state.last_error_kind, Some(2));
    }

    #[test]
    fn record_error_does_not_advance_backoff_timer() {
        let mut state = make_state();
        state.record_error(1, &FakeErr, "ctx");
        assert_eq!(
            state.next_backoff, INITIAL_BACKOFF,
            "record_error must not touch the backoff timer"
        );
    }

    #[test]
    fn record_error_marks_outage_in_progress() {
        let mut state = make_state();
        assert!(!state.is_in_outage());
        state.record_error(1, &FakeErr, "ctx");
        assert!(state.is_in_outage());
    }

    #[test]
    fn record_error_outage_cleared_by_on_recovery() {
        let mut state = make_state();
        state.record_error(1, &FakeErr, "ctx");
        assert!(state.on_recovery());
        assert!(!state.is_in_outage());
    }

    #[tokio::test(start_paused = true)]
    async fn on_error_delegates_deduplication_to_record_error() {
        // on_error must not log a second time for the same kind, even though it also sleeps.
        let mut state = make_state();
        on_err(&mut state, 1).await; // logs + sleeps
        // Calling record_error with the same kind should return false (already recorded).
        assert!(
            !state.record_error(1, &FakeErr, "ctx"),
            "on_error should have already recorded kind 1"
        );
    }
}
