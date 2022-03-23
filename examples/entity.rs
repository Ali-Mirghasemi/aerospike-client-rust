#[macro_use]
extern crate aerospike;

use std::env;
use std::sync::Arc;
use std::time::Instant;
use std::thread;

use aerospike::entity::bins::IntoBins;
use aerospike::entity::entity::Entity;
use aerospike::entity::entity_client::EntityClient;
use aerospike::entity::from_record::FromRecord;
use aerospike::entity::key::IntoKey;
use aerospike::entity::set::Set;
use aerospike::{Bins, Client, ClientPolicy, ReadPolicy, WritePolicy, Key, Bin, Record, Value};
use aerospike::operations;

#[derive(Debug, Clone, Default)]
struct UserModel {
    user_id:        i32,
    name:           String,
    height:         u8,
    permissions:    Vec<String>,
}

impl Set for UserModel {
    type Output = &'static str;

    fn namespace() -> Self::Output {
        "test"
    }

    fn set_name() -> Self::Output {
        "users"
    }
}

impl IntoKey for UserModel {
    type KeyType = i32;

    fn get_key(val: Self::KeyType) -> Key {
        as_key!(Self::namespace(), Self::set_name(), val)
    }
    
    fn key(&self) -> Key {
        Self::get_key(self.user_id)
    }

    fn into_key(self) -> Key {
        Self::get_key(self.user_id)
    }
}

impl<'a> IntoBins<'a> for UserModel {
    fn bins(entity: &Self) -> Vec<Bin<'a>> {
        vec![
            as_bin!("user_id", &entity.user_id),
            as_bin!("name", &entity.name),
            as_bin!("height", &entity.height),
            as_bin!("permissions", &entity.permissions),
        ]
    }

    fn into_bins(entity: Self) -> Vec<Bin<'a>> {
        vec![
            as_bin!("user_id", entity.user_id),
            as_bin!("name", entity.name),
            as_bin!("height", entity.height),
            as_bin!("permissions", entity.permissions),
        ]
    }
}

impl FromRecord for UserModel {
    fn from_record(record: Record) -> Self {
        UserModel {
            user_id:        from_rec!(record, "user_id"),
            name:           from_rec!(record, "name"),
            height:         from_rec!(record, "height"),
            permissions:    from_rec!(record, "permissions"),
        }
    }
}

impl<'a> Entity<'a> for UserModel {}

fn main() {
    let cpolicy = ClientPolicy::default();
    let hosts = env::var("AEROSPIKE_HOSTS")
        .unwrap_or(String::from("193.141.64.172:3000"));
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


