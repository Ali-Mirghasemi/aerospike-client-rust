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

use crate::Key;
use super::set::Set;

/// this trait used for convert a entity to ```Key```
pub trait IntoKey: Set + Sized {
    /// define key type
    type KeyType: Clone;

    /// get key for entity
    fn get_key(val: Self::KeyType) -> Key;
    
    /// convert entity by reference into ```Key```
    fn key(&self) -> Key;

    /// convert data type into ```Key```
    fn into_key(self) -> Key;
}

