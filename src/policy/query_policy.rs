// Copyright 2015-2016 Aerospike, Inc.
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

use std::time::Duration;
use std::error::Error;
use std::option::Option;
use std::u32;

use policy::{BasePolicy, PolicyLike};
use Priority;
use RecordExistsAction;
use GenerationPolicy;
use CommitLevel;

#[derive(Debug,Clone)]
pub struct QueryPolicy {
    pub base_policy: BasePolicy,

    pub max_concurrent_nodes: usize, // 0, parallel all

    pub record_queue_size: usize, // = 1024

    pub include_bin_data: bool, // = true

    pub include_ldt_data: bool, // = false

    pub fail_on_cluster_change: bool, // = true
}


impl QueryPolicy {
    pub fn new() -> Self {
        QueryPolicy::default()
    }
}

impl Default for QueryPolicy {
    fn default() -> QueryPolicy {
        QueryPolicy {
            base_policy: BasePolicy::default(),

            max_concurrent_nodes: 0,

            record_queue_size: 1024,

            include_bin_data: true,

            include_ldt_data: false,

            fail_on_cluster_change: true,
        }
    }
}

impl PolicyLike for QueryPolicy {
    fn base(&self) -> &BasePolicy {
        &self.base_policy
    }
}
