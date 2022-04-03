use aerospike::Entity;
use aerospike::entity::*;

use std::env;
use std::sync::Arc;
use std::time::Instant;
use std::thread;

use aerospike::{Bins, Client, ClientPolicy, ReadPolicy, WritePolicy, Key, Bin, Record, Value};

#[derive(Debug, Default, Entity)]
#[entity(namespace_fn = "get_namespace")]
#[entity(set_name = "users")]
struct UserModel {
    #[entity(key)]
    user_id:        i32,
    #[entity(rename="user_name")]
    name:           String,
    height:         u8,
    permissions:    Vec<String>,
    #[entity(ignore)]
    ignored:        i64,
}

// custom namespace function
fn get_namespace() -> &'static str {
    "test"
}

fn main() {
    let cpolicy = ClientPolicy::default();
    let hosts = env::var("AEROSPIKE_HOSTS")
        .unwrap_or(String::from("127.0.0.1:3000"));
    let client: EntityClient<UserModel> = EntityClient::new(&cpolicy, &hosts)
        .expect("Failed to connect to cluster");
    let client = Arc::new(client);
    
    let mut threads = vec![];
    let now = Instant::now();
    for i in 0..2 {
        let client = client.clone();
        let t = thread::spawn(move || {
            let rpolicy = ReadPolicy::default();
            let wpolicy = WritePolicy::default();
            let user_id = i;
            let user = UserModel {
                user_id,
                name: format!("user_{}", i),
                height: 100,
                permissions: vec!["One".to_owned(), "Two".to_owned(), "Three".to_owned()],
                ignored: 1000,
            };
            
            client.put(&wpolicy, &user).unwrap();
            let rec = client.get(&rpolicy, user_id);
            println!("Record: {:?}", rec.unwrap());

            client.touch(&wpolicy, user_id).unwrap();
            let rec = client.get_record(&rpolicy, user_id);
            println!("Record: {}", rec.unwrap());

            let rec = client.get_header(&rpolicy, user_id);
            println!("Record Header: {}", rec.unwrap());

            let exists = client.exists(&wpolicy, user_id).unwrap();
            println!("exists: {}", exists);

            let rec = client.update_field(&wpolicy, user_id, "height", Value::from(200 as u8));
            println!("operate: {:?}", rec.unwrap());

            let existed = client.delete(&wpolicy, user_id).unwrap();
            println!("existed (sould be true): {}", existed);

            let existed = client.delete(&wpolicy, user_id).unwrap();
            println!("existed (should be false): {}", existed);
        });

        threads.push(t);
    }

    for t in threads {
        t.join().unwrap();
    }

    println!("total time: {:?}", now.elapsed());
}
