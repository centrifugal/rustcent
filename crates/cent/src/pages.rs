//! Pages and stream positions (spec 3.8): the `*_all` methods reading every
//! item of a paged method, and `position()` on the results holding a stream
//! position.

use crate::{
    HistoryResult, MapReadStateResult, MapReadStreamResult, PublishResult, StreamPosition,
};

impl PublishResult {
    /// Where the publication went in the channel's stream: the `since` of a
    /// [`HistoryRequest`](crate::HistoryRequest) reading what came after it.
    #[must_use]
    pub fn position(&self) -> StreamPosition {
        StreamPosition::new()
            .offset(self.offset)
            .epoch(self.epoch.clone())
    }
}

impl HistoryResult {
    /// The channel's stream position when its history was read.
    #[must_use]
    pub fn position(&self) -> StreamPosition {
        StreamPosition::new()
            .offset(self.offset)
            .epoch(self.epoch.clone())
    }
}

impl MapReadStateResult {
    /// The map channel's stream position when its state was read: as
    /// [`MapReadStreamRequest`](crate::MapReadStreamRequest)'s `since_offset`
    /// and `since_epoch`, it reads the changes made after.
    #[must_use]
    pub fn position(&self) -> StreamPosition {
        StreamPosition::new()
            .offset(self.offset)
            .epoch(self.epoch.clone())
    }
}

impl MapReadStreamResult {
    /// The map channel's stream position when its stream was read.
    #[must_use]
    pub fn position(&self) -> StreamPosition {
        StreamPosition::new()
            .offset(self.offset)
            .epoch(self.epoch.clone())
    }
}

#[cfg(feature = "client")]
pub use pager::Pager;

#[cfg(feature = "client")]
mod pager {
    use std::fmt;

    use serde::de::DeserializeOwned;

    use crate::http_client::BoxFuture;
    use crate::{
        Client, Device, DeviceListRequest, DeviceListResult, DeviceTopic, DeviceTopicListRequest,
        DeviceTopicListResult, Error, HistoryRequest, HistoryResult, MapEntry, MapReadStateRequest,
        MapReadStateResult, Publication, Request, StreamPosition, UserTopic, UserTopicListRequest,
        UserTopicListResult,
    };

    /// The items of a paged method, every page of them, read a page at a
    /// time: what [`Client::history_all`], [`Client::device_list_all`] and
    /// the other `*_all` methods return.
    ///
    /// ```no_run
    /// # async fn f(client: cent::Client) -> Result<(), cent::Error> {
    /// let mut devices = client.device_list_all(cent::DeviceListRequest::new().limit(100));
    /// while let Some(device) = devices.next().await {
    ///     let device = device?;
    ///     println!("{}", device.id);
    /// }
    /// # Ok(()) }
    /// ```
    ///
    /// [`next`](Self::next) reads a page when it reaches it, not ahead, so a
    /// pager dropped part way reads no further page. An error ends the
    /// iteration: `next` returns it, after the items before it, and then
    /// `None`. A future of `next` dropped while reading a page - a
    /// `tokio::time::timeout` around it, say - loses nothing: the next call
    /// reads the same page again.
    ///
    /// Each page is a call of its own, bounded as the client's calls are: by
    /// [`with_timeout`](Client::with_timeout) each, by
    /// [`with_deadline`](Client::with_deadline) all together.
    pub struct Pager<T> {
        items: std::vec::IntoIter<T>,
        source: Option<Box<dyn Source<T>>>,
        refused: Option<Error>,
    }

    impl<T> fmt::Debug for Pager<T> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("Pager")
                .field("buffered", &self.items.len())
                .field("done", &self.source.is_none())
                .finish_non_exhaustive()
        }
    }

    impl<T: Send + 'static> Pager<T> {
        fn new(source: impl Source<T> + 'static) -> Self {
            Self {
                items: Vec::new().into_iter(),
                source: Some(Box::new(source)),
                refused: None,
            }
        }

        /// A pager which returns `error` and ends, having sent nothing.
        fn refused(error: Error) -> Self {
            Self {
                items: Vec::new().into_iter(),
                source: None,
                refused: Some(error),
            }
        }

        /// The next item: `Some(Ok(item))`, `Some(Err(error))` once when a
        /// page could not be read, and `None` after the last.
        pub async fn next(&mut self) -> Option<Result<T, Error>> {
            if let Some(e) = self.refused.take() {
                return Some(Err(e));
            }
            loop {
                if let Some(item) = self.items.next() {
                    return Some(Ok(item));
                }
                let source = self.source.as_mut()?;
                match source.fetch().await {
                    Ok(page) => {
                        if page.last {
                            self.source = None;
                        }
                        self.items = page.items.into_iter();
                    }
                    Err(e) => {
                        self.source = None;
                        return Some(Err(e));
                    }
                }
            }
        }
    }

    struct Page<T> {
        items: Vec<T>,
        /// No page follows.
        last: bool,
    }

    /// Where pages come from: a request, and how a reply gives the next.
    trait Source<T>: Send {
        fn fetch(&mut self) -> BoxFuture<'_, Result<Page<T>, Error>>;
    }

    /// A method paged by a cursor: `req`, then `req` with the cursor of
    /// each reply, until a reply has none.
    struct ByCursor<Req, Res, T> {
        client: Client,
        req: Req,
        page: fn(Res) -> (Vec<T>, String),
        set_cursor: fn(&mut Req, String),
    }

    impl<Req, Res, T> Source<T> for ByCursor<Req, Res, T>
    where
        Req: Request + Clone + Sync,
        Res: DeserializeOwned + Default + Send,
        T: Send,
    {
        fn fetch(&mut self) -> BoxFuture<'_, Result<Page<T>, Error>> {
            Box::pin(async move {
                let res: Res = self.client.call(self.req.clone()).await?;
                let (items, next) = (self.page)(res);
                let last = next.is_empty();
                if !last {
                    (self.set_cursor)(&mut self.req, next);
                }
                Ok(Page { items, last })
            })
        }
    }

    /// A channel's history, read forward from `since`: the next page is
    /// read from the last publication of the one before, and a page shorter
    /// than `limit` is the last.
    struct BySince {
        client: Client,
        req: HistoryRequest,
    }

    impl Source<Publication> for BySince {
        fn fetch(&mut self) -> BoxFuture<'_, Result<Page<Publication>, Error>> {
            Box::pin(async move {
                let res: HistoryResult = self.client.call(self.req.clone()).await?;
                let items = res.publications;
                let limit = usize::try_from(self.req.limit).unwrap_or(usize::MAX);
                let last = items.len() < limit;
                if let Some(p) = items.last() {
                    self.req.since = Some(StreamPosition::new().offset(p.offset).epoch(res.epoch));
                }
                Ok(Page { items, last })
            })
        }
    }

    impl Client {
        /// The publications of a channel's history, oldest first, `limit` at a
        /// time: from `since` when it is set, from the start of what the
        /// channel keeps otherwise. A page is read when the iteration reaches
        /// it, and the iteration ends after a page shorter than `limit`.
        ///
        /// ```no_run
        /// # async fn f(client: cent::Client, data: Box<cent::RawValue>) -> Result<(), cent::Error> {
        /// // Read what came after a publication.
        /// let published = client.publish(cent::PublishRequest::new("news").data(data)).await?;
        /// let mut after = client.history_all(
        ///     cent::HistoryRequest::new("news").limit(100).since(published.position()),
        /// );
        /// while let Some(publication) = after.next().await {
        ///     println!("{}", publication?.offset);
        /// }
        /// # Ok(()) }
        /// ```
        ///
        /// `limit` is the page size, so it must be 1 or more, and `reverse` is
        /// refused: `since` reads forward only. Either fails with
        /// [`Error::InvalidRequest`] before anything is sent. A publication
        /// made while iterating may or may not be seen. A position whose epoch
        /// is no longer the stream's - the history was lost, with a Redis
        /// restart say - fails with code 112,
        /// [`ApiError::UNRECOVERABLE_POSITION`](crate::ApiError::UNRECOVERABLE_POSITION):
        /// the publications after the last one seen are gone, and reading again
        /// from the start is the only way on.
        #[must_use]
        pub fn history_all(&self, request: HistoryRequest) -> Pager<Publication> {
            if request.limit < 1 {
                return Pager::refused(Error::invalid_request(
                    "history",
                    "history_all needs a limit of 1 or more: the page size",
                ));
            }
            if request.reverse {
                return Pager::refused(Error::invalid_request(
                    "history",
                    "history_all reads forward only: reverse is not supported",
                ));
            }
            Pager::new(BySince {
                client: self.clone(),
                req: request,
            })
        }

        /// Every device `request` selects, a page at a time, each page read
        /// when the iteration reaches it: `limit` is the page size, the
        /// server's default when unset. A device registered or removed while
        /// iterating may or may not be seen.
        ///
        /// Centrifugo PRO only.
        #[must_use]
        pub fn device_list_all(&self, request: DeviceListRequest) -> Pager<Device> {
            Pager::new(ByCursor {
                client: self.clone(),
                req: request,
                page: |r: DeviceListResult| (r.items, r.next_cursor),
                set_cursor: |r: &mut DeviceListRequest, c| r.cursor = c,
            })
        }

        /// Every device topic `request` selects, a page at a time, as
        /// [`device_list_all`](Self::device_list_all) reads devices.
        ///
        /// Centrifugo PRO only.
        #[must_use]
        pub fn device_topic_list_all(&self, request: DeviceTopicListRequest) -> Pager<DeviceTopic> {
            Pager::new(ByCursor {
                client: self.clone(),
                req: request,
                page: |r: DeviceTopicListResult| (r.items, r.next_cursor),
                set_cursor: |r: &mut DeviceTopicListRequest, c| r.cursor = c,
            })
        }

        /// Every user topic `request` selects, a page at a time, as
        /// [`device_list_all`](Self::device_list_all) reads devices.
        ///
        /// Centrifugo PRO only.
        #[must_use]
        pub fn user_topic_list_all(&self, request: UserTopicListRequest) -> Pager<UserTopic> {
            Pager::new(ByCursor {
                client: self.clone(),
                req: request,
                page: |r: UserTopicListResult| (r.items, r.next_cursor),
                set_cursor: |r: &mut UserTopicListRequest, c| r.cursor = c,
            })
        }

        /// Every entry of a map channel's state, a page at a time, each page
        /// read when the iteration reaches it: `limit` is the page size. An
        /// entry changed while iterating may be seen in either version, or
        /// not at all; for a consistent view, read the state with
        /// [`map_read_state`](Self::map_read_state), then the changes since its
        /// [`position`](MapReadStateResult::position) with
        /// [`map_read_stream`](Self::map_read_stream).
        #[must_use]
        pub fn map_read_state_all(&self, request: MapReadStateRequest) -> Pager<MapEntry> {
            Pager::new(ByCursor {
                client: self.clone(),
                req: request,
                page: |r: MapReadStateResult| (r.entries, r.cursor),
                set_cursor: |r: &mut MapReadStateRequest, c| r.cursor = c,
            })
        }
    }
}
