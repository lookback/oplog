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
    let uri = std::env::var("MONGO_URI").unwrap_or_else(|_| "mongodb://localhost".to_string());
    let client = Client::with_uri_str(&uri).await?;

    let mut oplog = Oplog::builder()
        .filter(doc! { "op": "i" })
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
