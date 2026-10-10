//! What a provider can see of its subscribers (P8), and where their state can
//! be kept (D11).

use std::future::poll_fn;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures_core::Stream;
use rbus_wire::event::Filter;

use crate::queue::{Popped, Queue};
use crate::subscribe::EventError;

/// One consumer's subscription to an element of a provider.
///
/// It serializes with serde, so a [`SubscriberStore`] can persist it.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub struct SubscriberInfo {
    /// The name subscribed to, as the consumer wrote it: an exact name, a
    /// table path, or a pattern with `*` or `[alias]`.
    pub event: String,
    /// The consumer's inbox, `rbus.<prog>.INBOX.<pid>` for C consumers.
    pub inbox: String,
    /// The consumer's component id, echoed in every event.
    pub component_id: i32,
    /// The consumer's filter, for value-change subscriptions (P12).
    pub filter: Option<Filter>,
    /// The id the engine gave the subscription.
    pub id: u32,
}

impl SubscriberInfo {
    /// A subscription with no filter. The engine gives subscriptions their
    /// ids; this is for stores and tests.
    pub fn new(
        event: impl Into<String>,
        inbox: impl Into<String>,
        component_id: i32,
        id: u32,
    ) -> Self {
        SubscriberInfo {
            event: event.into(),
            inbox: inbox.into(),
            component_id,
            filter: None,
            id,
        }
    }

    /// The same subscription with a filter.
    pub fn with_filter(mut self, filter: Filter) -> Self {
        self.filter = Some(filter);
        self
    }
}

/// A change to a provider's subscribers. Made by
/// [`Provided::subscription_events`](crate::provider::Provided::subscription_events).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum SubscriptionEvent {
    /// A consumer subscribed.
    Added(SubscriberInfo),
    /// A consumer unsubscribed, or disconnected.
    Removed(SubscriberInfo),
}

/// How many subscription events wait for a slow reader before the oldest are
/// dropped and reported as [`EventError::Lagged`].
pub(crate) const SUBSCRIPTION_EVENT_BUFFER: usize = 256;

/// Subscription changes as a stream. It ends when the provider is dropped.
pub struct SubscriptionEvents {
    pub(crate) queue: Arc<Queue<SubscriptionEvent>>,
}

impl SubscriptionEvents {
    /// A stream that has already ended.
    pub(crate) fn ended() -> Self {
        let queue = Arc::new(Queue::new(1, crate::queue::Overflow::DropOldest));
        queue.close();
        SubscriptionEvents { queue }
    }

    /// The next change; `None` once the provider is gone.
    pub async fn next(&mut self) -> Option<Result<SubscriptionEvent, EventError>> {
        poll_fn(|cx| self.poll_item(cx)).await
    }

    fn poll_item(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<SubscriptionEvent, EventError>>> {
        self.queue.poll_pop(cx).map(|p| {
            p.map(|p| match p {
                Popped::Item(e) => Ok(e),
                Popped::Lagged(n) => Err(EventError::Lagged(n)),
            })
        })
    }
}

impl Stream for SubscriptionEvents {
    type Item = Result<SubscriptionEvent, EventError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_item(cx)
    }
}

impl std::fmt::Debug for SubscriptionEvents {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubscriptionEvents").finish_non_exhaustive()
    }
}

/// Keeps a provider's subscribers across restarts (D11). The engine calls
/// [`SubscriberStore::save`] after every change and
/// [`SubscriberStore::load`] when the provider is registered. The default,
/// [`NoStore`], keeps nothing; a crash-recovery store (P17) plugs in here.
pub trait SubscriberStore: Send + Sync + 'static {
    /// Records the provider's current subscribers.
    fn save(&self, subscribers: &[SubscriberInfo]);

    /// The subscribers to restore when the provider is registered.
    fn load(&self) -> Vec<SubscriberInfo> {
        Vec::new()
    }
}

/// A [`SubscriberStore`] that keeps nothing.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoStore;

impl SubscriberStore for NoStore {
    fn save(&self, _subscribers: &[SubscriberInfo]) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::Overflow;

    #[tokio::test]
    async fn the_stream_reports_lag_and_ends() {
        let queue = Arc::new(Queue::new(1, Overflow::DropOldest));
        let mut events = SubscriptionEvents {
            queue: queue.clone(),
        };
        let info = SubscriberInfo {
            event: "A.B".into(),
            inbox: "i".into(),
            component_id: 1,
            filter: None,
            id: 3,
        };
        queue.push_now(SubscriptionEvent::Added(info.clone()));
        queue.push_now(SubscriptionEvent::Removed(info.clone()));
        assert!(matches!(
            events.next().await,
            Some(Err(EventError::Lagged(1)))
        ));
        assert_eq!(
            events.next().await,
            Some(Ok(SubscriptionEvent::Removed(info.clone())))
        );
        queue.close();
        assert!(events.next().await.is_none());
        assert!(SubscriptionEvents::ended().next().await.is_none());
        assert!(format!("{events:?}").contains("SubscriptionEvents"));
        NoStore.save(&[info]);
        assert!(NoStore.load().is_empty());
    }
}
