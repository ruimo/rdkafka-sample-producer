use std::{time::Duration};

use clap::Parser;
use model::User;
use rdkafka::{producer::{FutureProducer, FutureRecord, future_producer::OwnedDeliveryResult}, ClientConfig};

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    topic: String,
    #[arg(short, long, default_value="localhost:9092")]
    broker: String,
    user_name: String,
    email: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let user = model::User { user_name: args.user_name.clone(), email: args.email.clone() };

    send(&args, &user).await.expect("Cannot send message.");
}

async fn send(args: &Args, user: &User) -> OwnedDeliveryResult {
    let producer: &FutureProducer = &ClientConfig::new()
        .set("bootstrap.servers", &args.broker)
        .create()
        .expect("Producer creation error");

    let bytes = User::serialize(user).expect("Cannot serialize user.");

    producer.send(
        FutureRecord::to(&args.topic)
            .payload(&bytes)
            .key(&user.user_name),
            Duration::ZERO,
    ).await
}
