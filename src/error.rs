use mongodb::bson;
use std::fmt;

/// A type alias for convenience so we can fix the error to our own `Error` type.
pub type Result<T> = std::result::Result<T, Error>;

/// Error enumerates the list of possible error conditions when tailing an oplog.
#[derive(Debug)]
pub enum Error {
    /// A database connectivity error raised by the MongoDB driver.
    Database(mongodb::error::Error),
    /// An error when converting a BSON document to an `Operation` and it has a missing field or
    /// unexpected type.
    MissingField(bson::document::ValueAccessError),
    /// An error when converting a BSON document to an `Operation` and it has an unsupported
    /// operation type.
    UnknownOperation(String),
    /// An error when converting an applyOps command with invalid documents.
    InvalidOperation,
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Database(e) => Some(e),
            Error::MissingField(e) => Some(e),
            Error::UnknownOperation(_) => None,
            Error::InvalidOperation => None,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Database(ref err) => err.fmt(f),
            Error::MissingField(ref err) => err.fmt(f),
            Error::UnknownOperation(ref op) => write!(f, "Unknown operation type found: {}", op),
            Error::InvalidOperation => write!(f, "Invalid operation"),
        }
    }
}

impl From<bson::document::ValueAccessError> for Error {
    fn from(original: bson::document::ValueAccessError) -> Error {
        Error::MissingField(original)
    }
}

impl From<mongodb::error::Error> for Error {
    fn from(original: mongodb::error::Error) -> Error {
        Error::Database(original)
    }
}
