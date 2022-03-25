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


use std::{marker::PhantomData, sync::Arc};
use crate::{errors::Result, net::ToHosts, ClientPolicy, cluster::Node, Client};
use super::entity::Entity;
use crate::{ReadPolicy, Bins, WritePolicy, BatchRead, BatchPolicy, Record, operations, Bin, Value};
use super::{set::Set, key::IntoKey, bins::IntoBins, from_record::FromRecord};


pub struct EntityClient<'a, T> 
where
    T: Entity<'a>,
{
    inner:      crate::Client,
    entity:     PhantomData<&'a T>,
}

unsafe impl<'a, T: Entity<'a>> Send for EntityClient<'a, T> {}
unsafe impl<'a, T: Entity<'a>> Sync for EntityClient<'a, T> {}

impl<'a, T: Entity<'a>> EntityClient<'a, T> {

    /// Initializes Aerospike client with suitable hosts to seed the cluster map. The client policy
    /// is used to set defaults and size internal data structures. For each host connection that
    /// succeeds, the client will:
    ///
    /// - Add host to the cluster map
    /// - Request host's list of other nodes in cluster
    /// - Add these nodes to the cluster map
    ///
    /// In most cases, only one host is necessary to seed the cluster. The remaining hosts are
    /// added as future seeds in case of a complete network failure.
    ///
    /// If one connection succeeds, the client is ready to process database requests. If all
    /// connections fail and the policy's `fail_
    ///
    /// The seed hosts to connect to (one or more) can be specified as a comma-separated list of
    /// hostnames or IP addresses with optional port numbers, e.g.
    ///
    /// ```text
    /// 10.0.0.1:3000,10.0.0.2:3000,10.0.0.3:3000
    /// ```
    ///
    /// Port 3000 is used by default if the port number is omitted for any of the hosts.
    ///
    /// # Examples
    ///
    /// Using an environment variable to set the list of seed hosts.
    ///
    /// ```rust
    /// use aerospike::{Client, ClientPolicy};
    ///
    /// let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap();
    /// let client: EntityClient<Model> = EntityClient::new(&ClientPolicy::default(), &hosts).unwrap();
    /// ```
    pub fn new(policy: &ClientPolicy, hosts: &dyn ToHosts) -> Result<Self> {
        Ok(Self {
            inner: crate::Client::new(policy, hosts)?,
            entity: PhantomData,
        })
    }

    /// Closes the connection to the Aerospike cluster.
    pub fn close(&self) -> Result<()> {
        self.inner.close()
    }

    /// Returns `true` if the client is connected to any cluster nodes.
    pub fn is_connected(&self) -> bool {
        self.inner.is_connected()
    }

    /// Returns a list of the names of the active server nodes in the cluster.
    pub fn node_names(&self) -> Vec<String> {
        self.inner.node_names()
    }

    /// Return node given its name.
    pub fn get_node(&self, name: &str) -> Result<Arc<Node>> {
        self.inner.get_node(name)
    }

    /// Returns a list of active server nodes in the cluster.
    pub fn nodes(&self) -> Vec<Arc<Node>> {
        self.inner.nodes()
    }

    /// Read record for the specified key and convert to ```Self``` DataType
    pub fn get(&self, policy: &ReadPolicy, key: T::KeyType) -> Result<T> {
        T::get(&self.inner, policy, key)
    }

    pub fn get_record(&self, policy: &ReadPolicy, key: T::KeyType) -> Result<Record> {
        T::get_record(&self.inner, policy, key)
    }

    pub fn get_header(&self, policy: &ReadPolicy, key: T::KeyType) -> Result<Record> {
        T::get_header(&self.inner, policy, key)
    }

    pub fn batch_get(&self, policy: &BatchPolicy, key: &[T::KeyType]) -> Result<Vec<T>> {
        T::batch_get(&self.inner, policy, key)
    }

    /// Put entity into set
    pub fn put(&self, policy: &WritePolicy, entity: &T) -> Result<()> {
        T::put(&self.inner, policy, entity)
    }

    pub fn append(&self, policy: &WritePolicy, entity: &T) -> Result<()> {
        T::append(&self.inner, policy, entity)
    }

    pub fn prepend(&self, policy: &WritePolicy, entity: &T) -> Result<()> {
        T::prepend(&self.inner, policy, entity)
    }

    pub fn delete(&self, policy: &WritePolicy, key: T::KeyType) -> Result<bool> {
        T::delete(&self.inner, policy, key)
    }

    pub fn touch(&self, policy: &WritePolicy, key: T::KeyType) -> Result<()> {
        T::touch(&self.inner, policy, key)
    }

    pub fn exists(&self, policy: &WritePolicy, key: T::KeyType) -> Result<bool> {
        T::exists(&self.inner, policy, key)
    }

    pub fn update_field(&self, policy: &WritePolicy, key: T::KeyType, name: &str, val: Value) -> Result<T> {
        T::update_field(&self.inner, policy, key, name, val)
    }
}

impl<'a, T: Entity<'a>> AsRef<Client> for EntityClient<'a, T> {
    fn as_ref(&self) -> &Client {
        &self.inner
    }
}

impl<'a, T: Entity<'a>> From<Client> for EntityClient<'a, T> {
    fn from(inner: Client) -> Self {
        Self {
            inner,
            entity: PhantomData,
        }
    }
}

impl<'a, T: Entity<'a>> From<EntityClient<'a, T>> for Client {
    fn from(client: EntityClient<'a, T>) -> Self {
        client.inner
    }
}
