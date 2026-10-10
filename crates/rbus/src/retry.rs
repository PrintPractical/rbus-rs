//! [`Retry`], the backoff schedule shared by subscriptions and reconnects.

use std::time::Duration;

/// A retry schedule: a delay that doubles up to a limit, until a deadline.
///
/// It is used for retrying a subscription until its provider appears,
/// where only "no provider" is retried, and for reconnecting after the
/// broker goes away ([`RbusBuilder::reconnect`](crate::RbusBuilder::reconnect)).
///
/// ```
/// use std::time::Duration;
/// let retry = rbus::Retry::exponential().max_elapsed(Duration::from_secs(60));
/// // Never give up:
/// let forever = rbus::Retry::exponential().max_elapsed(Duration::MAX);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retry {
    pub(crate) initial: Duration,
    pub(crate) max_delay: Duration,
    pub(crate) max_elapsed: Duration,
}

impl Retry {
    /// C's schedule: 1 s, doubling, at most 60 s apart, giving up after 600 s
    /// (`rbus.c:5096, 5155-5185`).
    pub fn exponential() -> Retry {
        Retry {
            initial: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
            max_elapsed: Duration::from_secs(600),
        }
    }

    /// The first delay.
    pub fn initial(mut self, d: Duration) -> Retry {
        self.initial = d;
        self
    }

    /// The longest delay between attempts.
    pub fn max_delay(mut self, d: Duration) -> Retry {
        self.max_delay = d;
        self
    }

    /// Give up once this much time has passed.
    pub fn max_elapsed(mut self, d: Duration) -> Retry {
        self.max_elapsed = d;
        self
    }

    /// The first delay. Never zero, so a retry loop can't spin.
    pub(crate) fn first_delay(&self) -> Duration {
        self.initial.max(MIN_DELAY)
    }

    /// The delay after `d`: doubled, up to the maximum.
    pub(crate) fn next_delay(&self, d: Duration) -> Duration {
        d.saturating_mul(2).min(self.max_delay).max(MIN_DELAY)
    }

    /// True if waiting `next` more after `elapsed` goes past the deadline.
    pub(crate) fn gives_up(&self, elapsed: Duration, next: Duration) -> bool {
        elapsed.saturating_add(next) > self.max_elapsed
    }
}

impl Default for Retry {
    /// [`Retry::exponential`].
    fn default() -> Retry {
        Retry::exponential()
    }
}

/// The shortest delay between attempts, whatever the schedule says.
const MIN_DELAY: Duration = Duration::from_millis(1);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schedules_never_spin_or_overflow() {
        let zero = Retry::exponential()
            .initial(Duration::ZERO)
            .max_delay(Duration::ZERO);
        assert_eq!(zero.first_delay(), MIN_DELAY);
        assert_eq!(zero.next_delay(Duration::ZERO), MIN_DELAY);
        let huge = Retry::exponential()
            .initial(Duration::MAX)
            .max_delay(Duration::MAX)
            .max_elapsed(Duration::MAX);
        assert_eq!(huge.next_delay(Duration::MAX), Duration::MAX);
        assert!(!huge.gives_up(Duration::MAX, Duration::MAX));
        let r = Retry::exponential();
        assert_eq!(
            r.next_delay(Duration::from_secs(40)),
            Duration::from_secs(60)
        );
        assert!(r.gives_up(Duration::from_secs(590), Duration::from_secs(20)));
        assert_eq!(Retry::default(), r);
    }

    #[test]
    fn builder_sets_each_limit() {
        let r = Retry::exponential()
            .initial(Duration::from_millis(1))
            .max_delay(Duration::from_millis(2))
            .max_elapsed(Duration::from_millis(3));
        assert_eq!(
            (r.initial, r.max_delay, r.max_elapsed),
            (
                Duration::from_millis(1),
                Duration::from_millis(2),
                Duration::from_millis(3)
            )
        );
    }
}
