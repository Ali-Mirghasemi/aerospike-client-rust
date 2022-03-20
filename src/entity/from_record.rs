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

use crate::Record;

/// this trait help you convert result record into data type
pub trait FromRecord: Default {
    /// convert record into data type
    fn from_record(record: Record) -> Self;
}

#[macro_export]
macro_rules! from_rec {
    ($record:expr, $name:expr) => {
        From::from($record.bins.get($name).unwrap_or_default())
    };
}
