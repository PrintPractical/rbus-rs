//! [`Retry`], the backoff schedule shared by subscriptions and reconnects.

use std::time::Duration;

/// A retry schedule: a delay that doubles up to a limit, until a deadline.
///
/// It is used for retrying a subscription until its provider appears (E10),
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
