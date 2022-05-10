mod replication_tests;
mod replicator;
pub mod replicator_consumer;

use std::time::Duration;

use clap::Parser;
use futures_util::stream::FusedStream;
use futures_util::StreamExt;
use tokio::select;
use tokio::time::sleep;

use crate::replicator::Replicator;

#[derive(Parser)]
struct Args {
    /// Keyspace name
    #[clap(short, long)]
    keyspace: String,

    /// Table names provided as a comma delimited string
    #[clap(short, long)]
    table: String,

    /// Address of a node in source cluster
    #[clap(short, long)]
    source: String,

    /// Address of a node in destination cluster
    #[clap(short, long)]
    destination: String,

    /// Window size in seconds
    #[clap(long, default_value_t = 60.)]
    window_size: f64,

    /// Safety interval in seconds
    #[clap(long, default_value_t = 30.)]
    safety_interval: f64,

    /// Sleep interval in seconds
    #[clap(long, default_value_t = 10.)]
    sleep_interval: f64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let tables: Vec<_> = args.table.split(',').map(|s| s.to_string()).collect();
    let sleep_interval = Duration::from_secs_f64(args.sleep_interval);

    let (mut replicator, mut handles) = Replicator::new(
        args.source,
        args.keyspace.clone(),
        tables.clone(),
        args.destination,
        args.keyspace,
        tables,
        Duration::from_secs_f64(args.window_size),
        Duration::from_secs_f64(args.safety_interval),
        sleep_interval,
    )
    .await?;

    loop {
        select! {
            _ = tokio::signal::ctrl_c() => {
                println!("Shutting down...");
                replicator.stop();
                while handles.next().await.is_some() {}
                return Ok(())
            }
            res = handles.next(), if !handles.is_terminated() => {
                let res = res.unwrap();
                if res.is_err() {
                    replicator.stop();
                    while handles.next().await.is_some() {}
                    return res;
                }
            }
        }
        sleep(sleep_interval).await;
    }
}
