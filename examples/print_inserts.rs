use futures::StreamExt;
use mongodb::bson::doc;
use mongodb::Client;
use oplog::Oplog;
use std::process;

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("{:?}", e);
        process::exit(1);
    }
}

async fn run() -> oplog::Result<()> {
    let client = Client::with_uri_str("mongodb://localhost").await?;

    let mut oplog = Oplog::builder()
        .filter(Some(doc! { "op": "i" }))
        .build(&client)
        .await?;

    while let Some(res) = oplog.next().await {
        let oper = res?;
        println!("{:?}", oper);
    }

    eprintln!(
        "Oplog cursor ended. This probably means the oplog.rs collection \
        is empty. See https://jira.mongodb.org/browse/SERVER-13955"
    );

    process::exit(1);
}
