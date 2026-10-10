//! The bounded queue between the connection task and one event stream (D12).
//!
//! - **Drop-oldest** (the default): when full, the oldest item is dropped and
//!   counted; the stream reports the count as `Lagged(n)` before its next item.
//! - **Block**: when full, the connection task waits for room. This is real
//!   back-pressure: while it waits, nothing else on the connection is read.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::task::{Context, Poll, Waker};

use tokio::sync::Notify;

/// What a subscription does when its buffer is full.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum Overflow {
    /// Drop the oldest event; the stream yields `Err(EventError::Lagged(n))`
    /// before the next one.
    #[default]
    DropOldest,
    /// Stop reading from the broker until the consumer catches up. Every other
    /// reply and event on this connection waits too.
    Block,
}

struct State<T> {
    items: VecDeque<T>,
    lagged: u64,
    closed: bool,
    waker: Option<Waker>,
}

/// What a consumer takes from the queue.
pub(crate) enum Popped<T> {
    Item(T),
    Lagged(u64),
}

pub(crate) struct Queue<T> {
    state: Mutex<State<T>>,
    space: Notify,
    capacity: usize,
    overflow: Overflow,
}

impl<T> Queue<T> {
    pub(crate) fn new(capacity: usize, overflow: Overflow) -> Self {
        Queue {
            state: Mutex::new(State {
                items: VecDeque::new(),
                lagged: 0,
                closed: false,
                waker: None,
            }),
            space: Notify::new(),
            capacity: capacity.max(1),
            overflow,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State<T>> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn wake(state: &mut State<T>) {
        if let Some(w) = state.waker.take() {
            w.wake();
        }
    }

    /// Adds an item, honouring the overflow policy.
    pub(crate) async fn push(&self, item: T) {
        let mut item = Some(item);
        loop {
            let notified = self.space.notified();
            {
                let mut s = self.lock();
                if s.closed {
                    return;
                }
                if s.items.len() < self.capacity || self.overflow == Overflow::DropOldest {
                    if s.items.len() >= self.capacity {
                        s.items.pop_front();
                        s.lagged += 1;
                    }
                    s.items.push_back(item.take().expect("pushed once"));
                    Self::wake(&mut s);
                    return;
                }
            }
            notified.await;
        }
    }

    /// Adds an item without waiting: when full, drops the oldest item and
    /// counts it as lagged, whatever the overflow policy.
    pub(crate) fn push_now(&self, item: T) {
        let mut s = self.lock();
        if s.closed {
            return;
        }
        if s.items.len() >= self.capacity {
            s.items.pop_front();
            s.lagged += 1;
        }
        s.items.push_back(item);
        Self::wake(&mut s);
    }

    /// Adds an item ahead of everything queued (an initial value that must
    /// come before events that raced it). Never blocks; may exceed capacity by one.
    pub(crate) fn push_front(&self, item: T) {
        let mut s = self.lock();
        if !s.closed {
            s.items.push_front(item);
            Self::wake(&mut s);
        }
    }

    /// Ends the stream once the queued items are taken.
    pub(crate) fn close(&self) {
        let mut s = self.lock();
        s.closed = true;
        Self::wake(&mut s);
        drop(s);
        self.space.notify_waiters();
    }

    pub(crate) fn poll_pop(&self, cx: &mut Context<'_>) -> Poll<Option<Popped<T>>> {
        let mut s = self.lock();
        if s.lagged > 0 {
            let n = std::mem::take(&mut s.lagged);
            return Poll::Ready(Some(Popped::Lagged(n)));
        }
        if let Some(item) = s.items.pop_front() {
            drop(s);
            self.space.notify_one();
            return Poll::Ready(Some(Popped::Item(item)));
        }
        if s.closed {
            return Poll::Ready(None);
        }
        s.waker = Some(cx.waker().clone());
        Poll::Pending
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.lock().items.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::poll_fn;
    use std::sync::Arc;
    use std::time::Duration;

    async fn pop<T>(q: &Queue<T>) -> Option<Popped<T>> {
        poll_fn(|cx| q.poll_pop(cx)).await
    }

    fn item<T: std::fmt::Debug>(p: Option<Popped<T>>) -> T {
        match p {
            Some(Popped::Item(t)) => t,
            Some(Popped::Lagged(n)) => panic!("lagged {n}"),
            None => panic!("closed"),
        }
    }

    #[tokio::test]
    async fn drop_oldest_reports_lag_before_the_next_item() {
        let q = Queue::new(2, Overflow::DropOldest);
        for i in 0..5 {
            q.push(i).await;
        }
        assert!(matches!(pop(&q).await, Some(Popped::Lagged(3))));
        assert_eq!(item(pop(&q).await), 3);
        assert_eq!(item(pop(&q).await), 4);
        q.push_front(9);
        q.push(10).await;
        assert_eq!(item(pop(&q).await), 9);
        assert_eq!(item(pop(&q).await), 10);
        q.close();
        q.push(11).await;
        q.push_front(12);
        assert!(pop(&q).await.is_none());
    }

    #[tokio::test]
    async fn block_waits_for_room() {
        let q = Arc::new(Queue::new(1, Overflow::Block));
        q.push(1).await;
        let pusher = tokio::spawn({
            let q = q.clone();
            async move { q.push(2).await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!pusher.is_finished(), "push should wait while full");
        assert_eq!(q.len(), 1);
        assert_eq!(item(pop(&q).await), 1);
        pusher.await.unwrap();
        assert_eq!(item(pop(&q).await), 2);
        // A blocked push gives up when the queue closes.
        q.push(3).await;
        let pusher = tokio::spawn({
            let q = q.clone();
            async move { q.push(4).await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        q.close();
        pusher.await.unwrap();
    }

    #[tokio::test]
    async fn pop_waits_for_an_item() {
        let q = Arc::new(Queue::new(4, Overflow::default()));
        let popper = tokio::spawn({
            let q = q.clone();
            async move { item(pop(&q).await) }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        q.push("x").await;
        assert_eq!(popper.await.unwrap(), "x");
        let q2 = Arc::new(Queue::<u8>::new(0, Overflow::DropOldest));
        let popper = tokio::spawn({
            let q = q2.clone();
            async move { pop(&q).await.is_none() }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        q2.close();
        assert!(popper.await.unwrap());
    }
}
