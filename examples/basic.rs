#[macro_use]
extern crate aerospike;

use std::env;
use std::sync::Arc;
use std::time::Instant;
use std::thread;

use aerospike::{Bins, Client, ClientPolicy, ReadPolicy, WritePolicy};
use aerospike::operations;

fn main() {
    let cpolicy = ClientPolicy::default();
    let hosts = env::var("AEROSPIKE_HOSTS")
        .unwrap_or(String::from("127.0.0.1:3000"));
    let client = Client::new(&cpolicy, &hosts)
        .expect("Failed to connect to cluster");
    let client = Arc::new(client);

    let mut threads = vec![];
    let now = Instant::now();
    for i in 0..2 {
        let client = client.clone();
        let t = thread::spawn(move || {
            let rpolicy = ReadPolicy::default();
            let wpolicy = WritePolicy::default();
            let key = as_key!("test", "test", i);
            let bins = [
                as_bin!("int", 123),
                as_bin!("str", "Hello, World!"),
            ];

            client.put(&wpolicy, &key, &bins).unwrap();
            let rec = client.get(&rpolicy, &key, Bins::All);
            println!("Record: {}", rec.unwrap());

            client.touch(&wpolicy, &key).unwrap();
            let rec = client.get(&rpolicy, &key, Bins::All);
            println!("Record: {}", rec.unwrap());

            let rec = client.get(&rpolicy, &key, Bins::None);
            println!("Record Header: {}", rec.unwrap());

            let exists = client.exists(&wpolicy, &key).unwrap();
            println!("exists: {}", exists);

            let bin = as_bin!("int", 999);
            let ops = &vec![operations::put(&bin), operations::get()];
            let op_rec = client.operate(&wpolicy, &key, ops);
            println!("operate: {}", op_rec.unwrap());

            let existed = client.delete(&wpolicy, &key).unwrap();
            println!("existed (sould be true): {}", existed);

            let existed = client.delete(&wpolicy, &key).unwrap();
            println!("existed (should be false): {}", existed);
        });

        threads.push(t);
    }

    for t in threads {
        t.join().unwrap();
    }

    println!("total time: {:?}", now.elapsed());
}