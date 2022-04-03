// Copyright 2015-2018 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

use crate::{Client, ReadPolicy, Bins, WritePolicy, BatchRead, BatchPolicy, Record, operations, Bin, Value};
use crate::errors::Result;
use super::{set::Set, key::IntoKey, bins::IntoBins, from_record::FromRecord};

/// implement entity 
pub trait Entity<'a>: Set + IntoKey + IntoBins<'a> + FromRecord {
    
    /// Read record for the specified key and convert to ```Self``` DataType
    fn get(client: &Client, policy: &ReadPolicy, key: Self::KeyType) -> Result<Self> {
        client.get(
            policy, 
            &Self::get_key(key),
            Bins::All
        )
        .map(Self::from_record)
    }

    /// get record for the specified entity with given key
    fn get_record(client: &Client, policy: &ReadPolicy, key: Self::KeyType) -> Result<Record> {
        client.get(
            policy, 
            &Self::get_key(key),
            Bins::All
        )
    }

    /// get record header for the specified entity with given key
    fn get_header(client: &Client, policy: &ReadPolicy, key: Self::KeyType) -> Result<Record> {
        client.get(
            policy, 
            &Self::get_key(key),
            Bins::None
        )
    }

    /// get multiple records for the specified entity with given keys
    fn batch_get(client: &Client, policy: &BatchPolicy, key: &[Self::KeyType]) -> Result<Vec<Self>> {
        let batch_reads: Vec<BatchRead> = key.into_iter().map(|x| BatchRead::new(Self::get_key(x.clone()), &Bins::All)).collect();
        client.batch_get(
            policy, 
            batch_reads
        )
        .map(|x| {
            x.into_iter().filter_map(|x| {
                x.record.map(Self::from_record)
            })
            .collect()
        })
    }

    /// Put entity into set
    fn put(client: &Client, policy: &WritePolicy, entity: &Self) -> Result<()> {
        client.put(
            policy, 
            &Self::key(entity),
            &Self::bins(entity)
        )
    }

    /// append model into database
    fn append(client: &Client, policy: &WritePolicy, entity: &Self) -> Result<()> {
        client.append(
            policy, 
            &Self::key(entity),
            &Self::bins(entity)
        )
    }

    fn prepend(client: &Client, policy: &WritePolicy, entity: &Self) -> Result<()> {
        client.prepend(
            policy, 
            &Self::key(entity),
            &Self::bins(entity)
        )
    }

    fn delete(client: &Client, policy: &WritePolicy, key: Self::KeyType) -> Result<bool> {
        client.delete(
            policy, 
            &Self::get_key(key)
        )
    }

    fn touch(client: &Client, policy: &WritePolicy, key: Self::KeyType) -> Result<()> {
        client.touch(
            policy, 
            &Self::get_key(key)
        )
    }

    fn exists(client: &Client, policy: &WritePolicy, key: Self::KeyType) -> Result<bool> {
        client.exists(
            policy, 
            &Self::get_key(key)
        )
    }

    fn update_field(client: &Client, policy: &WritePolicy, key: Self::KeyType, name: &str, val: Value) -> Result<Self> {
        let bin = Bin::new(name, val);
        let ops = &vec![operations::put(&bin), operations::get()];
        client.operate(
            policy, 
            &Self::get_key(key), 
            ops
        )
        .map(Self::from_record)
    }

}

