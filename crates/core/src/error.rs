#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Input rejected before touching anything, e.g. a bad queue name or priority.
    #[error("{0}")]
    Invalid(String),

    #[error("{0} not found")]
    NotFound(String),

    /// The thing being created already exists.
    #[error("{0}")]
    Conflict(String),

    /// The queue hit `maxMessages`.
    #[error("{0}")]
    QueueFull(String),

    /// Free disk space is below `--min-free-disk`, so publishing is stopped.
    #[error("{0}")]
    DiskFull(String),

    /// The broker is shutting down and no longer takes new work.
    #[error("broker is shutting down")]
    Unavailable,

    #[error("storage error: {0}")]
    Storage(String),
}

impl Error {
    /// Stable machine-readable identifier, sent to HTTP clients alongside the message.
    pub fn kind(&self) -> &'static str {
        match self {
            Error::Invalid(_) => "invalid",
            Error::NotFound(_) => "notFound",
            Error::Conflict(_) => "conflict",
            Error::QueueFull(_) => "queueFull",
            Error::DiskFull(_) => "diskFull",
            Error::Unavailable => "unavailable",
            Error::Storage(_) => "storage",
        }
    }
}

impl From<redb::Error> for Error {
    fn from(err: redb::Error) -> Self {
        Error::Storage(err.to_string())
    }
}

macro_rules! storage_error {
    ($($ty:ty),*) => {
        $(impl From<$ty> for Error {
            fn from(err: $ty) -> Self {
                Error::Storage(err.to_string())
            }
        })*
    };
}

storage_error!(
    redb::DatabaseError,
    redb::TransactionError,
    redb::TableError,
    redb::StorageError,
    redb::CommitError,
    redb::SetDurabilityError,
    serde_json::Error,
    std::io::Error
);

pub type Result<T> = std::result::Result<T, Error>;
