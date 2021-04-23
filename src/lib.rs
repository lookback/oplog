#![warn(missing_docs)]

//! A library for iterating over a MongoDB replica set oplog.
//!
//! Given a MongoDB `Client` connected to a replica set, this crate allows you to iterate over an
//! `Oplog` as if it were a collection of statically typed `Operation`s.
//!
//! # Example
//!
//! At its most basic, an `Oplog` will yield _all_ operations in the oplog when iterated over:
//!
//! ```rust,no_run
//! use futures::StreamExt;
//! use mongodb::Client;
//! use oplog::Oplog;
//!
//! # async fn run() -> Result<(), oplog::Error> {
//! let client = Client::with_uri_str("mongodb://localhost").await?;
//!
//! let mut oplog = Oplog::new(&client).await?;
//!
//! while let Some(res) = oplog.next().await {
//!     let oper = res?;
//!     println!("{:?}", oper);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! Alternatively, an `Oplog` can be built with a filter via `OplogBuilder` to restrict the
//! operations yielded:
//!
//! ```rust,no_run
//! use futures::StreamExt;
//! use mongodb::bson::doc;
//! use mongodb::Client;
//! use oplog::Oplog;
//! use std::process;
//!
//! # async fn run() -> Result<(), oplog::Error> {
//! let client = Client::with_uri_str("mongodb://localhost").await?;
//!
//! let mut oplog = Oplog::builder()
//!     .filter(doc! { "op": "i" })
//!     .build(&client)
//!     .await?;
//!
//! while let Some(res) = oplog.next().await {
//!     let oper = res?;
//!     println!("{:?}", oper);
//! }
//!
//! # Ok(())
//! # }
//! ```

use bson::Document;
use futures::ready;
use futures::Stream;
use mongodb::options::{CursorType, FindOptions};
use mongodb::Client;
use mongodb::Cursor;
use std::pin::Pin;
use std::task::{Context, Poll};

pub use oper::Operation;

pub use mongodb;
pub use mongodb::bson;

mod error;
mod oper;

pub use error::{Error, Result};

/// Oplog represents a MongoDB replica set oplog.
///
/// It implements the `Iterator` trait so it can be iterated over, yielding successive `Operation`s
/// as they are read from the server. This will effectively iterate forever as it will await new
/// operations.
///
/// Any errors raised while tailing the oplog (e.g. a connectivity issue) will cause the iteration
/// to end.
pub struct Oplog {
    /// The internal MongoDB cursor for the current position in the oplog.
    cursor: Cursor<bson::Document>,
}

impl Oplog {
    /// Creates an instance with default options.
    pub async fn new(client: &Client) -> Result<Oplog> {
        OplogBuilder::new().build(client).await
    }

    /// Builder to configure the Oplog.
    pub fn builder() -> OplogBuilder {
        OplogBuilder::new()
    }
}

impl Stream for Oplog {
    type Item = Result<Operation>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if let Some(res) = ready!(Pin::new(&mut this.cursor).poll_next(cx)) {
            match res {
                Ok(v) => match Operation::new(&v) {
                    Ok(o) => Some(Ok(o)).into(),
                    Err(e) => Some(Err(e)).into(),
                },
                Err(e) => Some(Err(e.into())).into(),
            }
        } else {
            // Underlying cursor is over. This probably indicates that the oplog.rs collection
            // is empty. See https://jira.mongodb.org/browse/SERVER-13955
            None.into()
        }
    }
}

/// A builder for an `Oplog`.
///
/// This builder enables configuring a filter on the oplog so that only operations matching a given
/// criteria are returned (e.g. to set a start time or filter out unwanted operation types).
///
/// The lifetime `'a` refers to the lifetime of the MongoDB client.
#[derive(Clone)]
pub struct OplogBuilder {
    filter: Option<Document>,
    batch_size: Option<u32>,
}

impl OplogBuilder {
    pub(crate) fn new() -> OplogBuilder {
        OplogBuilder {
            filter: None,
            batch_size: None,
        }
    }

    /// Provide an optional filter for the oplog.
    ///
    /// This is empty by default so all operations are returned.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use mongodb::Client;
    /// use oplog::bson::doc;
    /// use oplog::Oplog;
    ///
    /// # async fn run() -> Result<(), oplog::Error> {
    /// let client = Client::with_uri_str("mongodb://localhost").await?;
    ///
    /// let mut oplog = Oplog::builder()
    ///     .filter(doc! { "op": "i" })
    ///     .build(&client)
    ///     .await?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn filter(mut self, filter: Document) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Set `batch_size` option on the underlying mongodb cursor.
    ///
    /// Default this is not set and falls back on whatever the default is.
    pub fn batch_size(mut self, batch_size: u32) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    /// Executes the query and builds the `Oplog` over the client provided.
    pub async fn build(self, client: &Client) -> Result<Oplog> {
        let coll = client.database("local").collection("oplog.rs");

        let opts = FindOptions::builder()
            .no_cursor_timeout(true)
            .cursor_type(CursorType::Tailable)
            .batch_size(self.batch_size)
            .build();

        let cursor = coll.find(self.filter, opts).await?;

        Ok(Oplog { cursor })
    }
}
