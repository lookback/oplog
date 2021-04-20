# oplog

A library for iterating over a MongoDB replica set oplog.

Given a MongoDB `Client` connected to a replica set, this crate allows you to iterate over an
`Oplog` as if it were a collection of statically typed `Operation`s.

## Example

At its most basic, an `Oplog` will yield _all_ operations in the oplog when iterated over:

```rust
use futures::StreamExt;
use mongodb::Client;
use oplog::Oplog;

let client = Client::with_uri_str("mongodb://localhost").await?;

let mut oplog = Oplog::new(&client).await?;

while let Some(res) = oplog.next().await {
    let oper = res?;
    println!("{:?}", oper);
}
```

Alternatively, an `Oplog` can be built with a filter via `OplogBuilder` to restrict the
operations yielded:

```rust
use futures::StreamExt;
use mongodb::bson::doc;
use mongodb::Client;
use oplog::Oplog;
use std::process;

let client = Client::with_uri_str("mongodb://localhost").await?;

let mut oplog = Oplog::builder()
    .filter(Some(doc! { "op": "i" }))
    .build(&client)
    .await?;

while let Some(res) = oplog.next().await {
    let oper = res?;
    println!("{:?}", oper);
}

```

License: MIT
