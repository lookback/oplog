//! The operation module is responsible for converting MongoDB BSON documents into specific
//! `Operation` types, one for each type of document stored in the MongoDB oplog. As much as
//! possible, we convert BSON types into more typical Rust types (e.g. BSON timestamps into UTC
//! datetimes).
//!
//! As we accept _any_ document, it may not be a valid operation so wrap any conversions in a
//! `Result`.

use std::fmt;

use crate::{Error, Result};
use bson::{Bson, Document};
use chrono::{DateTime, TimeZone, Utc};
use mongodb::bson;

/// A MongoDB oplog operation.
#[derive(Clone, Debug, PartialEq)]
pub enum Operation {
    /// A no-op as inserted periodically by MongoDB or used to initiate new replica sets.
    Noop {
        /// The time of the operation.
        timestamp: DateTime<Utc>,
        /// The message associated with this operation.
        message: Option<String>,
    },
    /// An insert of a document into a specific database and collection.
    Insert {
        /// The time of the operation.
        timestamp: DateTime<Utc>,
        /// The full namespace of the operation including its database and collection.
        namespace: String,
        /// The BSON document inserted into the namespace.
        document: Document,
    },
    /// An update of a document in a specific database and collection matching a given query.
    Update {
        /// The time of the operation.
        timestamp: DateTime<Utc>,
        /// The full namespace of the operation including its database and collection.
        namespace: String,
        /// The BSON selection criteria for the update.
        query: Document,
        /// The BSON update applied in this operation.
        update: Document,
    },
    /// The deletion of a document in a specific database and collection matching a given query.
    Delete {
        /// The time of the operation.
        timestamp: DateTime<Utc>,
        /// The full namespace of the operation including its database and collection.
        namespace: String,
        /// The BSON selection criteria for the delete.
        query: Document,
    },
    /// A command such as the creation or deletion of a collection.
    Command {
        /// The time of the operation.
        timestamp: DateTime<Utc>,
        /// The full namespace of the operation including its database and collection.
        namespace: String,
        /// The BSON command.
        command: Document,
    },
    /// A command to apply multiple oplog operations at once.
    ApplyOps {
        /// The time of the operation.
        timestamp: DateTime<Utc>,
        /// The full namespace of the operation including its database and collection.
        namespace: String,
        /// A vector of operations to apply.
        operations: Vec<Operation>,
    },
}

impl Operation {
    /// Try to create a new Operation from a BSON document.
    ///
    /// # Example
    ///
    /// ```
    /// # #[macro_use]
    /// # use oplog::bson::{self, Bson, doc};
    /// use oplog::Operation;
    ///
    /// # fn main() {
    /// let document = doc! {
    ///     "ts": Bson::Timestamp(bson::Timestamp {
    ///         time: 1479561394,
    ///         increment: 0,
    ///     }),
    ///     "v": 2,
    ///     "op": "i",
    ///     "ns": "foo.bar",
    ///     "o": {
    ///         "foo": "bar"
    ///     }
    /// };
    /// let operation = Operation::new(&document);
    /// # }
    /// ```
    pub fn new(document: &Document) -> Result<Operation> {
        let op = document.get_str("op")?;

        match op {
            "n" => Operation::from_noop(document),
            "i" => Operation::from_insert(document),
            "u" => Operation::from_update(document),
            "d" => Operation::from_delete(document),
            "c" => Operation::from_command(document),
            op => Err(Error::UnknownOperation(op.into())),
        }
    }

    /// Returns an operation from any BSON value.
    fn from_bson(bson: &Bson) -> Result<Operation> {
        match *bson {
            Bson::Document(ref document) => Operation::new(document),
            _ => Err(Error::InvalidOperation),
        }
    }

    /// Returns a no-op operation for a given document.
    fn from_noop(document: &Document) -> Result<Operation> {
        let ts = document.get_timestamp("ts")?;
        // We don't always get a document in "o"
        let message = document
            .get("o")
            .and_then(|d| d.as_document())
            .and_then(|d| d.get("msg"))
            .and_then(|d| d.as_str())
            .map(|s| s.to_string());

        Ok(Operation::Noop {
            timestamp: timestamp_to_datetime(ts),
            message,
        })
    }

    /// Return an insert operation for a given document.
    fn from_insert(document: &Document) -> Result<Operation> {
        let ts = document.get_timestamp("ts")?;
        let ns = document.get_str("ns")?;
        let o = document.get_document("o")?;

        Ok(Operation::Insert {
            timestamp: timestamp_to_datetime(ts),
            namespace: ns.into(),
            document: o.to_owned(),
        })
    }

    /// Return an update operation for a given document.
    fn from_update(document: &Document) -> Result<Operation> {
        let ts = document.get_timestamp("ts")?;
        let ns = document.get_str("ns")?;
        let o = document.get_document("o")?;
        let o2 = document.get_document("o2")?;

        Ok(Operation::Update {
            timestamp: timestamp_to_datetime(ts),
            namespace: ns.into(),
            query: o2.to_owned(),
            update: o.to_owned(),
        })
    }

    /// Return a delete operation for a given document.
    fn from_delete(document: &Document) -> Result<Operation> {
        let ts = document.get_timestamp("ts")?;
        let ns = document.get_str("ns")?;
        let o = document.get_document("o")?;

        Ok(Operation::Delete {
            timestamp: timestamp_to_datetime(ts),
            namespace: ns.into(),
            query: o.to_owned(),
        })
    }

    /// Return a command operation for a given document.
    ///
    /// Note that this can return either an `Operation::Command` or an `Operation::ApplyOps` when
    /// successful.
    fn from_command(document: &Document) -> Result<Operation> {
        let ts = document.get_timestamp("ts")?;
        let ns = document.get_str("ns")?;
        let o = document.get_document("o")?;

        match o.get_array("applyOps") {
            Ok(ops) => {
                let operations = ops
                    .iter()
                    .map(|bson| Operation::from_bson(bson))
                    .collect::<Result<Vec<Operation>>>()?;

                Ok(Operation::ApplyOps {
                    timestamp: timestamp_to_datetime(ts),
                    namespace: ns.into(),
                    operations: operations,
                })
            }
            Err(_) => Ok(Operation::Command {
                timestamp: timestamp_to_datetime(ts),
                namespace: ns.into(),
                command: o.to_owned(),
            }),
        }
    }
}

impl fmt::Display for Operation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Operation::Noop {
                timestamp,
                ref message,
            } => {
                write!(f, "No-op at {}: {:?}", timestamp, message)
            }
            Operation::Insert {
                timestamp,
                ref namespace,
                ref document,
            } => {
                write!(
                    f,
                    "Insert into {} at {}: {}",
                    namespace, timestamp, document
                )
            }
            Operation::Update {
                timestamp,
                ref namespace,
                ref query,
                ref update,
            } => {
                write!(
                    f,
                    "Update {} with {} at {}: {}",
                    namespace, query, timestamp, update
                )
            }
            Operation::Delete {
                timestamp,
                ref namespace,
                ref query,
            } => {
                write!(f, "Delete from {} at {}: {}", namespace, timestamp, query)
            }
            Operation::Command {
                timestamp,
                ref namespace,
                ref command,
            } => {
                write!(f, "Command  {} at {}: {}", namespace, timestamp, command)
            }
            Operation::ApplyOps {
                timestamp,
                ref namespace,
                ref operations,
            } => {
                write!(
                    f,
                    "ApplyOps {} at {}: {} operations",
                    namespace,
                    timestamp,
                    operations.len()
                )
            }
        }
    }
}

/// Convert a BSON timestamp into a UTC `DateTime`.
fn timestamp_to_datetime(timestamp: bson::Timestamp) -> DateTime<Utc> {
    let seconds = timestamp.time;
    let nanoseconds = timestamp.increment;

    Utc.timestamp(seconds as i64, nanoseconds)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    #[test]
    fn operation_converts_noops() {
        let doc = doc! {
            "ts" : Bson::Timestamp(bson::Timestamp {
                time: 1479419535 ,
                increment: 0,
            }),
            "v" : 2,
            "op" : "n",
            "ns" : "",
            "o" : {
                "msg" : "initiating set"
            }
        };
        let operation = Operation::new(&doc).unwrap();

        assert_eq!(
            operation,
            Operation::Noop {
                timestamp: Utc.timestamp(1479419535, 0),
                message: Some("initiating set".into()),
            }
        );
    }

    #[test]
    fn operation_converts_inserts() {
        let doc = doc! {
            "ts" : Bson::Timestamp(bson::Timestamp {
                time: 1479561394 ,
                increment:0
            }),
            "v" : 2,
            "op" : "i",
            "ns" : "foo.bar",
            "o" : {
                "foo" : "bar"
            }
        };
        let operation = Operation::new(&doc).unwrap();

        assert_eq!(
            operation,
            Operation::Insert {
                timestamp: Utc.timestamp(1479561394, 0),
                namespace: "foo.bar".into(),
                document: doc! { "foo" : "bar" },
            }
        );
    }

    #[test]
    fn operation_converts_updates() {
        let doc = doc! {
            "ts" : Bson::Timestamp(bson::Timestamp {
                time: 1479561033 ,
                increment: 0,
            }),
            "v" : 2,
            "op" : "u",
            "ns" : "foo.bar",
            "o2" : {
                "_id" : 1
            },
            "o" : {
                "$set" : {
                    "foo" : "baz"
                }
            }
        };
        let operation = Operation::new(&doc).unwrap();

        assert_eq!(
            operation,
            Operation::Update {
                timestamp: Utc.timestamp(1479561033, 0),
                namespace: "foo.bar".into(),
                query: doc! { "_id" : 1 },
                update: doc! { "$set" : { "foo" : "baz" } },
            }
        );
    }

    #[test]
    fn operation_converts_deletes() {
        let doc = doc! {
            "ts" : Bson::Timestamp(bson::Timestamp {
                time: 1479421186 ,
                increment: 0,
            }),
            "v" : 2,
            "op" : "d",
            "ns" : "foo.bar",
            "o" : {
                "_id" : 1
            }
        };
        let operation = Operation::new(&doc).unwrap();

        assert_eq!(
            operation,
            Operation::Delete {
                timestamp: Utc.timestamp(1479421186, 0),
                namespace: "foo.bar".into(),
                query: doc! { "_id" : 1 },
            }
        );
    }

    #[test]
    fn operation_converts_commands() {
        let doc = doc! {
            "ts" : Bson::Timestamp(bson::Timestamp {
                time: 1479553955 ,
                increment: 0,
            }),
            "v" : 2,
            "op" : "c",
            "ns" : "test.$cmd",
            "o" : {
                "create" : "foo"
            }
        };
        let operation = Operation::new(&doc).unwrap();

        assert_eq!(
            operation,
            Operation::Command {
                timestamp: Utc.timestamp(1479553955, 0),
                namespace: "test.$cmd".into(),
                command: doc! { "create" : "foo" },
            }
        );
    }

    #[test]
    fn operation_returns_unknown_operations() {
        let doc = doc! { "op" : "x" };
        let operation = Operation::new(&doc);

        match operation {
            Err(Error::UnknownOperation(op)) => assert_eq!(op, "x"),
            _ => panic!("Expected unknown operation."),
        }
    }

    #[test]
    fn operation_returns_missing_fields() {
        use bson::document::ValueAccessError;

        let doc = doc! { "foo" : "bar" };
        let operation = Operation::new(&doc);

        match operation {
            Err(Error::MissingField(err)) => assert_eq!(err, ValueAccessError::NotPresent),
            _ => panic!("Expected missing field."),
        }
    }

    #[test]
    fn operation_returns_apply_ops() {
        let doc = doc! {
            "ts" : Bson::Timestamp(bson::Timestamp {
                time: 1483789052 ,
                increment: 0,
            }),
            "op" : "c",
            "ns" : "foo.$cmd",
            "o" : {
                "applyOps" : [
                    {
                        "ts" : Bson::Timestamp(bson::Timestamp {
                            time: 1479561394 ,
                            increment: 0,
                        }),
                        "t" : 2,
                        "op" : "i",
                        "ns" : "foo.bar",
                        "o" : {
                            "_id" : 1,
                            "foo" : "bar"
                        }
                    }
                ]
            }
        };
        let operation = Operation::new(&doc).unwrap();

        assert_eq!(
            operation,
            Operation::ApplyOps {
                timestamp: Utc.timestamp(1483789052, 0),
                namespace: "foo.$cmd".into(),
                operations: vec![Operation::Insert {
                    timestamp: Utc.timestamp(1479561394, 0),
                    namespace: "foo.bar".into(),
                    document: doc! { "_id" : 1, "foo" : "bar" },
                }],
            }
        );
    }
}
