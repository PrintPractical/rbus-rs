//! Discovery through the broker.

use std::future::poll_fn;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures_core::Stream;
use rbus_wire::control::{self, DiscoveryReply, Expression, NameList, RegisteredComponents};
use serde::Serialize;

use crate::bus::Rbus;
use crate::error::{Error, Result};
use crate::name::{IntoName, Name};
use crate::queue::{Overflow, Popped, Queue};
use crate::subscribe::EventError;

/// Buffer of an advisory stream.
const ADVISORY_BUFFER: usize = 256;

/// A client joining or leaving the bus, as the broker announces it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Advisory {
    /// A client registered its inbox.
    Connected {
        /// The client's inbox, `rbus.<prog>.INBOX.<pid>` for C clients.
        inbox: String,
    },
    /// A client's connection closed.
    Disconnected {
        /// The client's inbox.
        inbox: String,
    },
}

impl Advisory {
    pub(crate) fn from_wire(a: control::Advisory) -> Advisory {
        // rtAdvisory.h: 0 = rtAdviseClientConnect, 1 = rtAdviseClientDisconnect.
        if a.event == 0 {
            Advisory::Connected { inbox: a.inbox }
        } else {
            Advisory::Disconnected { inbox: a.inbox }
        }
    }

    /// The client's inbox.
    pub fn inbox(&self) -> &str {
        match self {
            Advisory::Connected { inbox } | Advisory::Disconnected { inbox } => inbox,
        }
    }
}

/// Discovery queries. Made by [`Rbus::discover`].
#[derive(Debug, Clone, Copy)]
pub struct Discover<'a> {
    bus: &'a Rbus,
}

/// True for names that address a subtree: partial paths and wildcards.
pub(crate) fn is_wildcard(name: &str) -> bool {
    name.ends_with('.') || name.contains('*')
}

impl Rbus {
    /// Discovery queries: who owns what, and who is connected.
    pub fn discover(&self) -> Discover<'_> {
        Discover { bus: self }
    }

    async fn broker_query<T: Serialize>(&self, topic: &str, msg: &T) -> Result<Vec<String>> {
        let frame = self
            .inner
            .conn
            .broker_request(topic, msg)?
            .wait(self.inner.timeouts.broker, "discovery")
            .await?;
        let reply: DiscoveryReply = control::decode(&frame.payload)?;
        Ok(reply.items)
    }

    /// The owning component of each exact name (`_trace_origin_object`); the
    /// broker leaves unknown names out.
    pub(crate) async fn trace_origin<S: AsRef<str>>(&self, names: &[S]) -> Result<Vec<String>> {
        let list = NameList::new(names.iter().map(|n| n.as_ref().to_owned()).collect());
        self.broker_query(control::TOPIC_TRACE_ORIGIN, &list).await
    }

    /// The components with elements under a partial path or wildcard
    /// (`_RTROUTED.INBOX.QUERY`), without duplicates.
    pub(crate) async fn query_owners(&self, path: &str) -> Result<Vec<String>> {
        let expr = Expression {
            expression: path.to_owned(),
        };
        let mut owners = self.broker_query(control::TOPIC_QUERY, &expr).await?;
        let mut seen = std::collections::HashSet::new();
        owners.retain(|o| !o.is_empty() && seen.insert(o.clone()));
        Ok(owners)
    }
}

/// Where requests for a name go once its owners are known.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Route {
    /// To each owning component by name (subtrees found with QUERY).
    Components,
    /// To the name itself, which the broker routes to its owner.
    Name,
}

impl Rbus {
    /// The owners of `path`, and how to address them. As in C (`rbus_getExt`),
    /// a subtree is resolved with `_RTROUTED.INBOX.QUERY`; when that finds
    /// nothing (it only resolves paths that end in `.`), the name is looked
    /// up with `_trace_origin_object`, which also matches `*` and instance
    /// numbers against `{i}`, and requests go to the name itself.
    pub(crate) async fn resolve_owners(&self, path: &Name) -> Result<(Vec<String>, Route)> {
        if is_wildcard(path) {
            let owners = self.query_owners(path).await?;
            if !owners.is_empty() {
                return Ok((owners, Route::Components));
            }
        }
        let mut owners = self.trace_origin(&[path]).await?;
        owners.retain(|c| !c.is_empty());
        owners.dedup();
        if owners.is_empty() {
            return Err(Error::NotFound(path.clone()));
        }
        Ok((owners, Route::Name))
    }
}

impl Discover<'_> {
    /// The component that owns an element.
    ///
    /// ```no_run
    /// # async fn demo(bus: rbus::Rbus) -> rbus::Result<()> {
    /// let owner = bus.discover().owner("Device.WiFi.Radio.1.Enable").await?;
    /// # Ok(()) }
    /// ```
    pub async fn owner(&self, name: impl IntoName<Name>) -> Result<String> {
        let name = name.into_name()?;
        self.owners(name.clone())
            .await?
            .into_iter()
            .next()
            .ok_or(Error::NotFound(name))
    }

    /// Every component with elements under `path`: one for an exact name,
    /// possibly several for a partial path or wildcard. Fails with
    /// [`Error::NotFound`] when there are none.
    pub async fn owners(&self, path: impl IntoName<Name>) -> Result<Vec<String>> {
        let path = path.into_name()?;
        Ok(self.bus.resolve_owners(&path).await?.0)
    }

    /// The elements a component has registered.
    pub async fn elements(&self, component: &str) -> Result<Vec<Name>> {
        let expr = Expression {
            expression: component.to_owned(),
        };
        let items = self
            .bus
            .broker_query(control::TOPIC_ENUMERATE_ELEMENTS, &expr)
            .await?;
        // The broker lists the component's own route first.
        items
            .into_iter()
            .filter(|i| i != component)
            .map(|i| Name::parse(i).map_err(|e| Error::Protocol(e.to_string())))
            .collect()
    }

    /// Every component registered with the broker. Client inboxes and
    /// broker topics are left out.
    pub async fn components(&self) -> Result<Vec<String>> {
        let mut items = self
            .bus
            .broker_query(
                control::TOPIC_REGISTERED_COMPONENTS,
                &RegisteredComponents::default(),
            )
            .await?;
        items.retain(|i| !i.contains(".INBOX.") && !i.starts_with('_'));
        Ok(items)
    }

    /// A stream of clients connecting to and disconnecting from the bus.
    /// It ends when the connection closes.
    pub fn advisories(&self) -> Advisories {
        let queue = Arc::new(Queue::new(ADVISORY_BUFFER, Overflow::DropOldest));
        if self.bus.inner.conn.listen_advisories(&queue).is_err() {
            queue.close();
        }
        Advisories {
            queue,
            _bus: self.bus.clone(),
        }
    }
}

/// Broker advisories as a stream. Made by [`Discover::advisories`].
pub struct Advisories {
    queue: Arc<Queue<Advisory>>,
    _bus: Rbus,
}

impl Advisories {
    /// The next advisory; `None` once the connection is closed.
    pub async fn next(&mut self) -> Option<Result<Advisory, EventError>> {
        poll_fn(|cx| self.poll_item(cx)).await
    }

    fn poll_item(&self, cx: &mut Context<'_>) -> Poll<Option<Result<Advisory, EventError>>> {
        self.queue.poll_pop(cx).map(|p| {
            p.map(|p| match p {
                Popped::Item(a) => Ok(a),
                Popped::Lagged(n) => Err(EventError::Lagged(n)),
            })
        })
    }
}

impl Stream for Advisories {
    type Item = Result<Advisory, EventError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_item(cx)
    }
}

impl std::fmt::Debug for Advisories {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Advisories").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advisories_from_the_wire() {
        let c = Advisory::from_wire(control::Advisory {
            event: 0,
            inbox: "rbus.x.INBOX.1".into(),
        });
        assert_eq!(c.inbox(), "rbus.x.INBOX.1");
        assert!(matches!(c, Advisory::Connected { .. }));
        let d = Advisory::from_wire(control::Advisory {
            event: 1,
            inbox: "i".into(),
        });
        assert!(matches!(d, Advisory::Disconnected { .. }));
        assert_eq!(d.inbox(), "i");
    }

    #[test]
    fn wildcards() {
        assert!(is_wildcard("Device.WiFi."));
        assert!(is_wildcard("Device.IP.Interface.*.Status"));
        assert!(!is_wildcard("Device.IP.Interface.1.Status"));
    }
}
