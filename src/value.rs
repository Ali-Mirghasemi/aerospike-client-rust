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

use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::result::Result as StdResult;
use std::{f32, f64};

use byteorder::{ByteOrder, NetworkEndian};

use ripemd160::digest::Digest;
use ripemd160::Ripemd160;

use std::vec::Vec;

use crate::commands::buffer::Buffer;
use crate::commands::ParticleType;
use crate::errors::Result;
use crate::msgpack::{decoder, encoder};

#[cfg(feature = "serialization")]
use serde::ser::{SerializeMap, SerializeSeq};
#[cfg(feature = "serialization")]
use serde::{Serialize, Serializer};

/// Container for floating point bin values stored in the Aerospike database.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum FloatValue {
    /// Container for single precision float values.
    F32(u32),
    /// Container for double precision float values.
    F64(u64),
}

impl From<FloatValue> for f64 {
    fn from(val: FloatValue) -> f64 {
        match val {
            FloatValue::F32(_) => panic!(
                "This library does not automatically convert f32 -> f64 to be used in keys \
                 or bins."
            ),
            FloatValue::F64(val) => f64::from_bits(val),
        }
    }
}

impl<'a> From<&'a FloatValue> for f64 {
    fn from(val: &FloatValue) -> f64 {
        match *val {
            FloatValue::F32(_) => panic!(
                "This library does not automatically convert f32 -> f64 to be used in keys \
                 or bins."
            ),
            FloatValue::F64(val) => f64::from_bits(val),
        }
    }
}

impl From<f64> for FloatValue {
    fn from(val: f64) -> FloatValue {
        let mut val = val;
        if val.is_nan() {
            val = f64::NAN
        } // make all NaNs have the same representation
        FloatValue::F64(val.to_bits())
    }
}

impl<'a> From<&'a f64> for FloatValue {
    fn from(val: &f64) -> FloatValue {
        let mut val = *val;
        if val.is_nan() {
            val = f64::NAN
        } // make all NaNs have the same representation
        FloatValue::F64(val.to_bits())
    }
}

impl From<FloatValue> for f32 {
    fn from(val: FloatValue) -> f32 {
        match val {
            FloatValue::F32(val) => f32::from_bits(val),
            FloatValue::F64(val) => f32::from_bits(val as u32),
        }
    }
}

impl<'a> From<&'a FloatValue> for f32 {
    fn from(val: &FloatValue) -> f32 {
        match *val {
            FloatValue::F32(val) => f32::from_bits(val),
            FloatValue::F64(val) => f32::from_bits(val as u32),
        }
    }
}

impl From<f32> for FloatValue {
    fn from(val: f32) -> FloatValue {
        let mut val = val;
        if val.is_nan() {
            val = f32::NAN
        } // make all NaNs have the same representation
        FloatValue::F32(val.to_bits())
    }
}

impl<'a> From<&'a f32> for FloatValue {
    fn from(val: &f32) -> FloatValue {
        let mut val = *val;
        if val.is_nan() {
            val = f32::NAN
        } // make all NaNs have the same representation
        FloatValue::F32(val.to_bits())
    }
}

impl fmt::Display for FloatValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> StdResult<(), fmt::Error> {
        match *self {
            FloatValue::F32(val) => {
                let val: f32 = f32::from_bits(val);
                write!(f, "{}", val)
            }
            FloatValue::F64(val) => {
                let val: f64 = f64::from_bits(val);
                write!(f, "{}", val)
            }
        }
    }
}

/// Container for bin values stored in the Aerospike database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    /// Empty value.
    Nil,

    /// Boolean value.
    Bool(bool),

    /// Integer value. All integers are represented as 64-bit numerics in Aerospike.
    Int(i64),

    /// Unsigned integer value. The largest integer value that can be stored in a record bin is
    /// `i64::max_value()`; however the list and map data types can store integer values (and keys)
    /// up to `u64::max_value()`.
    ///
    /// # Panics
    ///
    /// Attempting to store an `u64` value as a record bin value will cause a panic. Use casting to
    /// store and retrieve `u64` values.
    UInt(u64),

    /// Floating point value. All floating point values are stored in 64-bit IEEE-754 format in
    /// Aerospike. Aerospike server v3.6.0 and later support double data type.
    Float(FloatValue),

    /// String value.
    String(String),

    /// Byte array value.
    Blob(Vec<u8>),

    /// List data type is an ordered collection of values. Lists can contain values of any
    /// supported data type. List data order is maintained on writes and reads.
    List(Vec<Value>),

    /// Map data type is a collection of key-value pairs. Each key can only appear once in a
    /// collection and is associated with a value. Map keys and values can be any supported data
    /// type.
    HashMap(HashMap<Value, Value>),

    /// Map data type where the map entries are sorted based key ordering (K-ordered maps) and may
    /// have an additional value-order index depending the namespace configuration (KV-ordered
    /// maps).
    OrderedMap(Vec<(Value, Value)>),

    /// GeoJSON data type are JSON formatted strings to encode geospatial information.
    GeoJSON(String),

    /// HLL value
    HLL(Vec<u8>),
}

#[allow(clippy::derive_hash_xor_eq)]
impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match *self {
            Value::Nil => {
                let v: Option<u8> = None;
                v.hash(state)
            }
            Value::Bool(ref val) => val.hash(state),
            Value::Int(ref val) => val.hash(state),
            Value::UInt(ref val) => val.hash(state),
            Value::Float(ref val) => val.hash(state),
            Value::String(ref val) | Value::GeoJSON(ref val) => val.hash(state),
            Value::Blob(ref val) | Value::HLL(ref val) => val.hash(state),
            Value::List(ref val) => val.hash(state),
            Value::HashMap(_) => panic!("HashMaps cannot be used as map keys."),
            Value::OrderedMap(_) => panic!("OrderedMaps cannot be used as map keys."),
        }
    }
}

impl Value {
    /// Returns true if this value is the empty value (nil).
    pub const fn is_nil(&self) -> bool {
        matches!(*self, Value::Nil)
    }

    /// Return the particle type for the value used in the wire protocol.
    /// For internal use only.
    #[doc(hidden)]
    pub fn particle_type(&self) -> ParticleType {
        match *self {
            Value::Nil => ParticleType::NULL,
            Value::Int(_) | Value::Bool(_) => ParticleType::INTEGER,
            Value::UInt(_) => panic!(
                "Aerospike does not support u64 natively on server-side. Use casting to \
                 store and retrieve u64 values."
            ),
            Value::Float(_) => ParticleType::FLOAT,
            Value::String(_) => ParticleType::STRING,
            Value::Blob(_) => ParticleType::BLOB,
            Value::List(_) => ParticleType::LIST,
            Value::HashMap(_) => ParticleType::MAP,
            Value::OrderedMap(_) => panic!("The library never passes ordered maps to the server."),
            Value::GeoJSON(_) => ParticleType::GEOJSON,
            Value::HLL(_) => ParticleType::HLL,
        }
    }

    /// Returns a string representation of the value.
    pub fn as_string(&self) -> String {
        match *self {
            Value::Nil => "<null>".to_string(),
            Value::Int(ref val) => val.to_string(),
            Value::UInt(ref val) => val.to_string(),
            Value::Bool(ref val) => val.to_string(),
            Value::Float(ref val) => val.to_string(),
            Value::String(ref val) | Value::GeoJSON(ref val) => val.to_string(),
            Value::Blob(ref val) | Value::HLL(ref val) => format!("{:?}", val),
            Value::List(ref val) => format!("{:?}", val),
            Value::HashMap(ref val) => format!("{:?}", val),
            Value::OrderedMap(ref val) => format!("{:?}", val),
        }
    }

    /// Calculate the size in bytes that the representation on wire for this value will require.
    /// For internal use only.
    #[doc(hidden)]
    pub fn estimate_size(&self) -> Result<usize> {
        match *self {
            Value::Nil => Ok(0),
            Value::Int(_) | Value::Bool(_) | Value::Float(_) => Ok(8),
            Value::UInt(_) => panic!(
                "Aerospike does not support u64 natively on server-side. Use casting to \
                 store and retrieve u64 values."
            ),
            Value::String(ref s) => Ok(s.len()),
            Value::Blob(ref b) => Ok(b.len()),
            Value::List(_) | Value::HashMap(_) => encoder::pack_value(&mut None, self),
            Value::OrderedMap(_) => panic!("The library never passes ordered maps to the server."),
            Value::GeoJSON(ref s) => Ok(1 + 2 + s.len()), // flags + ncells + jsonstr
            Value::HLL(ref h) => Ok(h.len()),
        }
    }

    /// Serialize the value into the given buffer.
    /// For internal use only.
    #[doc(hidden)]
    pub fn write_to(&self, buf: &mut Buffer) -> Result<usize> {
        match *self {
            Value::Nil => Ok(0),
            Value::Int(ref val) => buf.write_i64(*val),
            Value::UInt(_) => panic!(
                "Aerospike does not support u64 natively on server-side. Use casting to \
                 store and retrieve u64 values."
            ),
            Value::Bool(ref val) => buf.write_bool(*val),
            Value::Float(ref val) => buf.write_f64(f64::from(val)),
            Value::String(ref val) => buf.write_str(val),
            Value::Blob(ref val) | Value::HLL(ref val) => buf.write_bytes(val),
            Value::List(_) | Value::HashMap(_) => encoder::pack_value(&mut Some(buf), self),
            Value::OrderedMap(_) => panic!("The library never passes ordered maps to the server."),
            Value::GeoJSON(ref val) => buf.write_geo(val),
        }
    }

    /// Serialize the value as a record key.
    /// For internal use only.
    #[doc(hidden)]
    pub fn write_key_bytes(&self, h: &mut Ripemd160) -> Result<()> {
        match *self {
            Value::Int(ref val) => {
                let mut buf = [0; 8];
                NetworkEndian::write_i64(&mut buf, *val);
                h.input(&buf);
                Ok(())
            }
            Value::String(ref val) => {
                h.input(val.as_bytes());
                Ok(())
            }
            Value::Blob(ref val) => {
                h.input(val);
                Ok(())
            }
            _ => panic!("Data type is not supported as Key value."),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> StdResult<(), fmt::Error> {
        write!(f, "{}", self.as_string())
    }
}

impl Default for Value {
    fn default() -> Self {
        Value::Nil
    }
}

impl Default for &Value {
    fn default() -> &'static Value {
        &Value::Nil
    }
}

impl From<String> for Value {
    fn from(val: String) -> Value {
        Value::String(val)
    }
}

impl From<Vec<u8>> for Value {
    fn from(val: Vec<u8>) -> Value {
        Value::Blob(val)
    }
}

impl From<Vec<Value>> for Value {
    fn from(val: Vec<Value>) -> Value {
        Value::List(val)
    }
}

impl From<HashMap<Value, Value>> for Value {
    fn from(val: HashMap<Value, Value>) -> Value {
        Value::HashMap(val)
    }
}

impl From<f32> for Value {
    fn from(val: f32) -> Value {
        Value::Float(FloatValue::from(val))
    }
}

impl From<f64> for Value {
    fn from(val: f64) -> Value {
        Value::Float(FloatValue::from(val))
    }
}

impl From<FloatValue> for Value {
    fn from(val: FloatValue) -> Value {
        Value::Float(val)
    }
}

impl<'a> From<&'a f32> for Value {
    fn from(val: &'a f32) -> Value {
        Value::Float(FloatValue::from(*val))
    }
}

impl<'a> From<&'a f64> for Value {
    fn from(val: &'a f64) -> Value {
        Value::Float(FloatValue::from(*val))
    }
}

impl<'a> From<&'a FloatValue> for Value {
    fn from(val: &'a FloatValue) -> Value {
        Value::Float(*val)
    }
}

impl<'a> From<&'a String> for Value {
    fn from(val: &'a String) -> Value {
        Value::String(val.clone())
    }
}

impl<'a> From<&'a str> for Value {
    fn from(val: &'a str) -> Value {
        Value::String(val.to_string())
    }
}

impl<'a> From<&'a Vec<u8>> for Value {
    fn from(val: &'a Vec<u8>) -> Value {
        Value::Blob(val.clone())
    }
}

impl<'a> From<&'a [u8]> for Value {
    fn from(val: &'a [u8]) -> Value {
        Value::Blob(val.to_vec())
    }
}

impl From<bool> for Value {
    fn from(val: bool) -> Value {
        Value::Bool(val)
    }
}

impl From<i8> for Value {
    fn from(val: i8) -> Value {
        Value::Int(i64::from(val))
    }
}

impl From<u8> for Value {
    fn from(val: u8) -> Value {
        Value::Int(i64::from(val))
    }
}

impl From<i16> for Value {
    fn from(val: i16) -> Value {
        Value::Int(i64::from(val))
    }
}

impl From<u16> for Value {
    fn from(val: u16) -> Value {
        Value::Int(i64::from(val))
    }
}

impl From<i32> for Value {
    fn from(val: i32) -> Value {
        Value::Int(i64::from(val))
    }
}

impl From<u32> for Value {
    fn from(val: u32) -> Value {
        Value::Int(i64::from(val))
    }
}

impl From<i64> for Value {
    fn from(val: i64) -> Value {
        Value::Int(val)
    }
}

impl From<u64> for Value {
    fn from(val: u64) -> Value {
        Value::UInt(val)
    }
}

impl From<isize> for Value {
    fn from(val: isize) -> Value {
        Value::Int(val as i64)
    }
}

impl From<usize> for Value {
    fn from(val: usize) -> Value {
        Value::UInt(val as u64)
    }
}

impl<'a> From<&'a i8> for Value {
    fn from(val: &'a i8) -> Value {
        Value::Int(i64::from(*val))
    }
}

impl<'a> From<&'a u8> for Value {
    fn from(val: &'a u8) -> Value {
        Value::Int(i64::from(*val))
    }
}

impl<'a> From<&'a i16> for Value {
    fn from(val: &'a i16) -> Value {
        Value::Int(i64::from(*val))
    }
}

impl<'a> From<&'a u16> for Value {
    fn from(val: &'a u16) -> Value {
        Value::Int(i64::from(*val))
    }
}

impl<'a> From<&'a i32> for Value {
    fn from(val: &'a i32) -> Value {
        Value::Int(i64::from(*val))
    }
}

impl<'a> From<&'a u32> for Value {
    fn from(val: &'a u32) -> Value {
        Value::Int(i64::from(*val))
    }
}

impl<'a> From<&'a i64> for Value {
    fn from(val: &'a i64) -> Value {
        Value::Int(*val)
    }
}

impl<'a> From<&'a u64> for Value {
    fn from(val: &'a u64) -> Value {
        Value::UInt(*val)
    }
}

impl<'a> From<&'a isize> for Value {
    fn from(val: &'a isize) -> Value {
        Value::Int(*val as i64)
    }
}

impl<'a> From<&'a usize> for Value {
    fn from(val: &'a usize) -> Value {
        Value::UInt(*val as u64)
    }
}

impl<'a> From<&'a bool> for Value {
    fn from(val: &'a bool) -> Value {
        Value::Bool(*val)
    }
}

impl From<Option<i8>> for Value {
    fn from(val: Option<i8>) -> Self {
        val.map_or(Value::Nil, Value::from)
    }
}

impl From<Option<u8>> for Value {
    fn from(val: Option<u8>) -> Self {
        val.map_or(Value::Nil, Value::from)
    }
}

impl From<Option<i16>> for Value {
    fn from(val: Option<i16>) -> Self {
        val.map_or(Value::Nil, Value::from)
    }
}

impl From<Option<u16>> for Value {
    fn from(val: Option<u16>) -> Self {
        val.map_or(Value::Nil, Value::from)
    }
}

impl From<Option<i32>> for Value {
    fn from(val: Option<i32>) -> Self {
        val.map_or(Value::Nil, Value::from)
    }
}

impl From<Option<u32>> for Value {
    fn from(val: Option<u32>) -> Self {
        val.map_or(Value::Nil, Value::from)
    }
}

impl From<Option<i64>> for Value {
    fn from(val: Option<i64>) -> Self {
        val.map_or(Value::Nil, Value::from)
    }
}

impl From<Option<u64>> for Value {
    fn from(val: Option<u64>) -> Self {
        val.map_or(Value::Nil, Value::from)
    }
}

impl From<Option<isize>> for Value {
    fn from(val: Option<isize>) -> Self {
        val.map_or(Value::Nil, Value::from)
    }
}

impl From<Option<usize>> for Value {
    fn from(val: Option<usize>) -> Self {
        val.map_or(Value::Nil, Value::from)
    }
}

impl From<Option<String>> for Value {
    fn from(val: Option<String>) -> Self {
        val.map_or(Value::Nil, Value::from)
    }
}

impl From<Option<Vec<u8>>> for Value {
    fn from(val: Option<Vec<u8>>) -> Self {
        val.map_or(Value::Nil, Value::from)
    }
}

impl From<Option<bool>> for Value {
    fn from(val: Option<bool>) -> Self {
        val.map_or(Value::Nil, Value::from)
    }
}

impl<'a> From<&'a Option<i8>> for Value {
    fn from(val: &'a Option<i8>) -> Self {
        val.map_or(Value::Nil, Value::from)
    }
}

impl<'a> From<&'a Option<u8>> for Value {
    fn from(val: &'a Option<u8>) -> Self {
        val.map_or(Value::Nil, Value::from)
    }
}

impl<'a> From<&'a Option<i16>> for Value {
    fn from(val: &'a Option<i16>) -> Self {
        val.map_or(Value::Nil, Value::from)
    }
}

impl<'a> From<&'a Option<u16>> for Value {
    fn from(val: &'a Option<u16>) -> Self {
        val.map_or(Value::Nil, Value::from)
    }
}

impl<'a> From<&'a Option<i32>> for Value {
    fn from(val: &'a Option<i32>) -> Self {
        val.map_or(Value::Nil, Value::from)
    }
}

impl<'a> From<&'a Option<u32>> for Value {
    fn from(val: &'a Option<u32>) -> Self {
        val.map_or(Value::Nil, Value::from)
    }
}

impl<'a> From<&'a Option<i64>> for Value {
    fn from(val: &'a Option<i64>) -> Self {
        val.map_or(Value::Nil, Value::from)
    }
}

impl<'a> From<&'a Option<isize>> for Value {
    fn from(val: &'a Option<isize>) -> Self {
        val.map_or(Value::Nil, Value::from)
    }
}

impl<'a> From<&'a Option<usize>> for Value {
    fn from(val: &'a Option<usize>) -> Self {
        val.map_or(Value::Nil, Value::from)
    }
}

impl<'a> From<&'a Option<bool>> for Value {
    fn from(val: &'a Option<bool>) -> Self {
        val.map_or(Value::Nil, Value::from)
    }
}

impl<'a> From<&'a Option<String>> for Value {
    fn from(val: &'a Option<String>) -> Self {
        match val {
            Some(val) => Value::from(val.clone()),
            _ => Value::Nil,
        }
    }
}

impl<'a> From<&'a Option<Vec<u8>>> for Value {
    fn from(val: &'a Option<Vec<u8>>) -> Self {
        match val {
            Some(val) => Value::from(val.clone()),
            _ => Value::Nil,
        }
    }
}

impl From<Value> for i8 {
    fn from(val: Value) -> i8 {
        match val {
            Value::Int(val) => val as i8,
            Value::UInt(val) => val as i8,
            _ => panic!("Value is not an integer to convert."),
        }
    }
}

impl From<Value> for u8 {
    fn from(val: Value) -> u8 {
        match val {
            Value::Int(val) => val as u8,
            Value::UInt(val) => val as u8,
            _ => panic!("Value is not an integer to convert."),
        }
    }
}

impl From<Value> for i16 {
    fn from(val: Value) -> i16 {
        match val {
            Value::Int(val) => val as i16,
            Value::UInt(val) => val as i16,
            _ => panic!("Value is not an integer to convert."),
        }
    }
}

impl From<Value> for u16 {
    fn from(val: Value) -> u16 {
        match val {
            Value::Int(val) => val as u16,
            Value::UInt(val) => val as u16,
            _ => panic!("Value is not an integer to convert."),
        }
    }
}

impl From<Value> for i32 {
    fn from(val: Value) -> i32 {
        match val {
            Value::Int(val) => val as i32,
            Value::UInt(val) => val as i32,
            _ => panic!("Value is not an integer to convert."),
        }
    }
}

impl From<Value> for u32 {
    fn from(val: Value) -> u32 {
        match val {
            Value::Int(val) => val as u32,
            Value::UInt(val) => val as u32,
            _ => panic!("Value is not an integer to convert."),
        }
    }
}

impl From<Value> for i64 {
    fn from(val: Value) -> i64 {
        match val {
            Value::Int(val) => val,
            Value::UInt(val) => val as i64,
            _ => panic!("Value is not an integer to convert."),
        }
    }
}

impl From<Value> for u64 {
    fn from(val: Value) -> u64 {
        match val {
            Value::Int(val) => val as u64,
            Value::UInt(val) => val,
            _ => panic!("Value is not an integer to convert."),
        }
    }
}

impl From<Value> for isize {
    fn from(val: Value) -> isize {
        match val {
            Value::Int(val) => val as isize,
            Value::UInt(val) => val as isize,
            _ => panic!("Value is not an integer to convert."),
        }
    }
}

impl From<Value> for usize {
    fn from(val: Value) -> usize {
        match val {
            Value::Int(val) => val as usize,
            Value::UInt(val) => val as usize,
            _ => panic!("Value is not an integer to convert."),
        }
    }
}

impl From<Value> for bool {
    fn from(val: Value) -> bool {
        match val {
            Value::Bool(val) => val,
            _ => panic!("Value is not an integer to convert."),
        }
    }
}

impl From<Value> for String {
    fn from(val: Value) -> String {
        match val {
            Value::String(val) => val,
            Value::GeoJSON(val) => val,
            _ => panic!("Value is not an integer to convert."),
        }
    }
}

impl From<Value> for Vec<Value> {
    fn from(val: Value) -> Vec<Value> {
        match val {
            Value::List(val) => val,
            _ => panic!("Value is not an integer to convert."),
        }
    }
}

impl From<Value> for Vec<u8> {
    fn from(val: Value) -> Vec<u8> {
        match val {
            Value::Blob(val) => val,
            _ => panic!("Value is not an integer to convert."),
        }
    }
}

impl From<Value> for FloatValue {
    fn from(val: Value) -> FloatValue {
        match val {
            Value::Float(val) => val,
            _ => panic!("Value is not an integer to convert."),
        }
    }
}

impl From<Value> for f32 {
    fn from(val: Value) -> f32 {
        match val {
            Value::Float(val) => f32::from(val),
            _ => panic!("Value is not an integer to convert."),
        }
    }
}

impl From<Value> for f64 {
    fn from(val: Value) -> f64 {
        match val {
            Value::Float(val) => f64::from(val),
            _ => panic!("Value is not an integer to convert."),
        }
    }
}

impl From<Value> for Option<i8> {
    fn from(val: Value) -> Self {
        match val {
            Value::Int(val) => Some(val as i8),
            Value::UInt(val) => Some(val as i8),
            _ => None,
        }
    }
}

impl From<Value> for Option<u8> {
    fn from(val: Value) -> Self {
        match val {
            Value::Int(val) => Some(val as u8),
            Value::UInt(val) => Some(val as u8),
            _ => None,
        }
    }
}

impl From<Value> for Option<i16> {
    fn from(val: Value) -> Self {
        match val {
            Value::Int(val) => Some(val as i16),
            Value::UInt(val) => Some(val as i16),
            _ => None,
        }
    }
}

impl From<Value> for Option<u16> {
    fn from(val: Value) -> Self {
        match val {
            Value::Int(val) => Some(val as u16),
            Value::UInt(val) => Some(val as u16),
            _ => None,
        }
    }
}

impl From<Value> for Option<i32> {
    fn from(val: Value) -> Self {
        match val {
            Value::Int(val) => Some(val as i32),
            Value::UInt(val) => Some(val as i32),
            _ => None,
        }
    }
}

impl From<Value> for Option<u32> {
    fn from(val: Value) -> Self {
        match val {
            Value::Int(val) => Some(val as u32),
            Value::UInt(val) => Some(val as u32),
            _ => None,
        }
    }
}

impl From<Value> for Option<i64> {
    fn from(val: Value) -> Self {
        match val {
            Value::Int(val) => Some(val),
            Value::UInt(val) => Some(val as i64),
            _ => None,
        }
    }
}

impl From<Value> for Option<u64> {
    fn from(val: Value) -> Self {
        match val {
            Value::Int(val) => Some(val as u64),
            Value::UInt(val) => Some(val),
            _ => None,
        }
    }
}

impl From<Value> for Option<isize> {
    fn from(val: Value) -> Self {
        match val {
            Value::Int(val) => Some(val as isize),
            Value::UInt(val) => Some(val as isize),
            _ => None,
        }
    }
}

impl From<Value> for Option<usize> {
    fn from(val: Value) -> Self {
        match val {
            Value::Int(val) => Some(val as usize),
            Value::UInt(val) => Some(val as usize),
            _ => None,
        }
    }
}

impl From<Value> for Option<bool> {
    fn from(val: Value) -> Self {
        match val {
            Value::Bool(val) => Some(val),
            _ => None,
        }
    }
}

impl From<Value> for Option<String> {
    fn from(val: Value) -> Self {
        match val {
            Value::String(val) => Some(val),
            Value::GeoJSON(val) => Some(val),
            _ => None,
        }
    }
}

impl From<Value> for Option<Vec<u8>> {
    fn from(val: Value) -> Self {
        match val {
            Value::Blob(val) => Some(val),
            _ => None,
        }
    }
}

impl From<Value> for Option<Vec<Value>> {
    fn from(val: Value) -> Self {
        match val {
            Value::List(val) => Some(val),
            _ => None,
        }
    }
}

impl From<Value> for Option<FloatValue> {
    fn from(val: Value) -> Self {
        match val {
            Value::Float(val) => Some(val),
            _ => None,
        }
    }
}

impl From<Value> for Option<f32> {
    fn from(val: Value) -> Self {
        match val {
            Value::Float(val) => Some(f32::from(val)),
            _ => None,
        }
    }
}

impl From<Value> for Option<f64> {
    fn from(val: Value) -> Self {
        match val {
            Value::Float(val) => Some(f64::from(val)),
            _ => None,
        }
    }
}

impl<'a> From<&'a Value> for i8 {
    fn from(val: &'a Value) -> i8 {
        match *val {
            Value::Int(val) => val as i8,
            Value::UInt(val) => val as i8,
            _ => panic!("Value is not an integer to convert."),
        }
    }
}

impl<'a> From<&'a Value> for u8 {
    fn from(val: &'a Value) -> u8 {
        match *val {
            Value::Int(val) => val as u8,
            Value::UInt(val) => val as u8,
            _ => panic!("Value is not an integer to convert."),
        }
    }
}

impl<'a> From<&'a Value> for i16 {
    fn from(val: &'a Value) -> i16 {
        match *val {
            Value::Int(val) => val as i16,
            Value::UInt(val) => val as i16,
            _ => panic!("Value is not an integer to convert."),
        }
    }
}

impl<'a> From<&'a Value> for u16 {
    fn from(val: &'a Value) -> u16 {
        match *val {
            Value::Int(val) => val as u16,
            Value::UInt(val) => val as u16,
            _ => panic!("Value is not an integer to convert."),
        }
    }
}

impl<'a> From<&'a Value> for i32 {
    fn from(val: &'a Value) -> i32 {
        match *val {
            Value::Int(val) => val as i32,
            Value::UInt(val) => val as i32,
            _ => panic!("Value is not an integer to convert."),
        }
    }
}

impl<'a> From<&'a Value> for u32 {
    fn from(val: &'a Value) -> u32 {
        match *val {
            Value::Int(val) => val as u32,
            Value::UInt(val) => val as u32,
            _ => panic!("Value is not an integer to convert."),
        }
    }
}

impl<'a> From<&'a Value> for i64 {
    fn from(val: &'a Value) -> i64 {
        match *val {
            Value::Int(val) => val,
            Value::UInt(val) => val as i64,
            _ => panic!("Value is not an integer to convert."),
        }
    }
}

impl<'a> From<&'a Value> for u64 {
    fn from(val: &'a Value) -> u64 {
        match *val {
            Value::Int(val) => val as u64,
            Value::UInt(val) => val as u64,
            _ => panic!("Value is not an integer to convert."),
        }
    }
}

impl<'a> From<&'a Value> for isize {
    fn from(val: &'a Value) -> isize {
        match *val {
            Value::Int(val) => val as isize,
            Value::UInt(val) => val as isize,
            _ => panic!("Value is not an integer to convert."),
        }
    }
}

impl<'a> From<&'a Value> for usize {
    fn from(val: &'a Value) -> usize {
        match *val {
            Value::Int(val) => val as usize,
            Value::UInt(val) => val as usize,
            _ => panic!("Value is not an integer to convert."),
        }
    }
}

impl<'a> From<&'a Value> for bool {
    fn from(val: &'a Value) -> bool {
        match *val {
            Value::Bool(val) => val,
            _ => panic!("Value is not a bool to convert."),
        }
    }
}

impl<'a> From<&'a Value> for String {
    fn from(val: &'a Value) -> String {
        match val {
            Value::String(val) => val.clone(),
            Value::GeoJSON(val) => val.clone(),
            _ => panic!("Value is not a string to convert."),
        }
    }
}

impl<'a> From<&'a Value> for Vec<Value> {
    fn from(val: &'a Value) -> Vec<Value> {
        match val {
            Value::List(val) => val.clone(),
            _ => panic!("Value is not a list to convert."),
        }
    }
}

impl<'a> From<&'a Value> for Vec<u8> {
    fn from(val: &'a Value) -> Vec<u8> {
        match val {
            Value::Blob(val) => val.clone(),
            _ => panic!("Value is not a blob to convert."),
        }
    }
}

impl<'a> From<&'a Value> for FloatValue {
    fn from(val: &'a Value) -> FloatValue {
        match val {
            Value::Float(val) => *val,
            _ => panic!("Value is not a Float to convert."),
        }
    }
}

impl<'a> From<&'a Value> for f32 {
    fn from(val: &'a Value) -> f32 {
        match val {
            Value::Float(val) => f32::from(val),
            _ => panic!("Value is not a Float to convert."),
        }
    }
}

impl<'a> From<&'a Value> for f64 {
    fn from(val: &'a Value) -> f64 {
        match val {
            Value::Float(val) => f64::from(val),
            _ => panic!("Value is not a float to convert."),
        }
    }
}

impl<'a> From<&'a Value> for Option<i8> {
    fn from(val: &'a Value) -> Self {
        match *val {
            Value::Int(val) => Some(val as i8),
            Value::UInt(val) => Some(val as i8),
            _ => None,
        }
    }
}

impl<'a> From<&'a Value> for Option<u8> {
    fn from(val: &'a Value) -> Self {
        match *val {
            Value::Int(val) => Some(val as u8),
            Value::UInt(val) => Some(val as u8),
            _ => None,
        }
    }
}

impl<'a> From<&'a Value> for Option<i16> {
    fn from(val: &'a Value) -> Self {
        match *val {
            Value::Int(val) => Some(val as i16),
            Value::UInt(val) => Some(val as i16),
            _ => None,
        }
    }
}

impl<'a> From<&'a Value> for Option<u16> {
    fn from(val: &'a Value) -> Self {
        match *val {
            Value::Int(val) => Some(val as u16),
            Value::UInt(val) => Some(val as u16),
            _ => None,
        }
    }
}

impl<'a> From<&'a Value> for Option<i32> {
    fn from(val: &'a Value) -> Self {
        match *val {
            Value::Int(val) => Some(val as i32),
            Value::UInt(val) => Some(val as i32),
            _ => None,
        }
    }
}

impl<'a> From<&'a Value> for Option<u32> {
    fn from(val: &'a Value) -> Self {
        match *val {
            Value::Int(val) => Some(val as u32),
            Value::UInt(val) => Some(val as u32),
            _ => None,
        }
    }
}

impl<'a> From<&'a Value> for Option<i64> {
    fn from(val: &'a Value) -> Self {
        match *val {
            Value::Int(val) => Some(val),
            Value::UInt(val) => Some(val as i64),
            _ => None,
        }
    }
}

impl<'a> From<&'a Value> for Option<u64> {
    fn from(val: &'a Value) -> Self {
        match *val {
            Value::Int(val) => Some(val as u64),
            Value::UInt(val) => Some(val),
            _ => None,
        }
    }
}

impl<'a> From<&'a Value> for Option<isize> {
    fn from(val: &'a Value) -> Self {
        match *val {
            Value::Int(val) => Some(val as isize),
            Value::UInt(val) => Some(val as isize),
            _ => None,
        }
    }
}

impl<'a> From<&'a Value> for Option<usize> {
    fn from(val: &'a Value) -> Self {
        match *val {
            Value::Int(val) => Some(val as usize),
            Value::UInt(val) => Some(val as usize),
            _ => None,
        }
    }
}

impl<'a> From<&'a Value> for Option<bool> {
    fn from(val: &'a Value) -> Self {
        match *val {
            Value::Bool(val) => Some(val),
            _ => None,
        }
    }
}

impl<'a> From<&'a Value> for Option<String> {
    fn from(val: &'a Value) -> Self {
        match val {
            Value::String(val) => Some(val.clone()),
            Value::GeoJSON(val) => Some(val.clone()),
            _ => None,
        }
    }
}

impl<'a> From<&'a Value> for Option<Vec<u8>> {
    fn from(val: &'a Value) -> Self {
        match val {
            Value::Blob(val) => Some(val.clone()),
            _ => None,
        }
    }
}

impl<'a> From<&'a Value> for Option<Vec<Value>> {
    fn from(val: &'a Value) -> Self {
        match val {
            Value::List(val) => Some(val.clone()),
            _ => None,
        }
    }
}

impl<'a> From<&'a Value> for Option<FloatValue> {
    fn from(val: &'a Value) -> Self {
        match *val {
            Value::Float(val) => Some(val),
            _ => None,
        }
    }
}

impl<'a> From<&'a Value> for Option<f32> {
    fn from(val: &'a Value) -> Self {
        match val {
            Value::Float(val) => Some(f32::from(val)),
            _ => None,
        }
    }
}

impl<'a> From<&'a Value> for Option<f64> {
    fn from(val: &'a Value) -> Self {
        match val {
            Value::Float(val) => Some(f64::from(val)),
            _ => None,
        }
    }
}

impl From<Vec<String>> for Value {
    fn from(val: Vec<String>) -> Self {
        Value::List(val.into_iter().map(Value::from).collect())
    }
}

impl<'a> From<&'a Vec<String>> for Value {
    fn from(val: &'a Vec<String>) -> Self {
        Value::List(val.iter().map(Value::from).collect())
    }
}

impl From<Option<Vec<String>>> for Value {
    fn from(val: Option<Vec<String>>) -> Self {
        val.map_or(
            Value::Nil, 
            |val| Value::List(val.into_iter().map(Value::from).collect())
        )
    }
}

impl<'a> From<&'a Option<Vec<String>>> for Value {
    fn from(val: &'a Option<Vec<String>>) -> Self {
        match val {
            Some(val) => Value::List(val.into_iter().map(Value::from).collect()),
            _ => Value::Nil,
        }
    }
}

impl From<Value> for Vec<String> {
    fn from(val: Value) -> Self {
        match val {
            Value::List(val) => val.into_iter().map(String::from).collect(),
            _ => panic!("Value is not a string list to convert."),
        }
    }
}

impl<'a> From<&'a Value> for Vec<String> {
    fn from(val: &'a Value) -> Self {
        match val {
            Value::List(val) => val.iter().map(String::from).collect(),
            _ => panic!("Value is not a string list to convert."),
        }
    }
}

impl From<Value> for Option<Vec<String>> {
    fn from(val: Value) -> Self {
        match val {
            Value::List(val) => Some(val.into_iter().map(String::from).collect()),
            _ => None,
        }
    }
}

impl<'a> From<&'a Value> for Option<Vec<String>> {
    fn from(val: &'a Value) -> Self {
        match val {
            Value::List(val) => Some(val.iter().map(String::from).collect()),
            _ => None,
        }
    }
}

#[doc(hidden)]
pub fn bytes_to_particle(ptype: u8, buf: &mut Buffer, len: usize) -> Result<Value> {
    match ParticleType::from(ptype) {
        ParticleType::NULL => Ok(Value::Nil),
        ParticleType::INTEGER => {
            let val = buf.read_i64(None)?;
            Ok(Value::Int(val))
        }
        ParticleType::FLOAT => {
            let val = buf.read_f64(None)?;
            Ok(Value::Float(FloatValue::from(val)))
        }
        ParticleType::STRING => {
            let val = buf.read_str(len)?;
            Ok(Value::String(val))
        }
        ParticleType::GEOJSON => {
            buf.skip(1)?;
            let ncells = buf.read_i16(None)? as usize;
            let header_size: usize = ncells * 8;

            buf.skip(header_size)?;
            let val = buf.read_str(len - header_size - 3)?;
            Ok(Value::GeoJSON(val))
        }
        ParticleType::BLOB => Ok(Value::Blob(buf.read_blob(len)?)),
        ParticleType::LIST => {
            let val = decoder::unpack_value_list(buf)?;
            Ok(val)
        }
        ParticleType::MAP => {
            let val = decoder::unpack_value_map(buf)?;
            Ok(val)
        }
        ParticleType::DIGEST => Ok(Value::from("A DIGEST, NOT IMPLEMENTED YET!")),
        ParticleType::LDT => Ok(Value::from("A LDT, NOT IMPLEMENTED YET!")),
        ParticleType::HLL => Ok(Value::HLL(buf.read_blob(len)?)),
    }
}

/// Constructs a new Value from one of the supported native data types.
#[macro_export]
macro_rules! as_val {
    ($val:expr) => {{
        $crate::Value::from($val)
    }};
}

/// Constructs a new `GeoJSON` Value from one of the supported native data types.
#[macro_export]
macro_rules! as_geo {
    ($val:expr) => {{
        $crate::Value::GeoJSON($val.to_owned())
    }};
}

/// Constructs a new Blob Value from one of the supported native data types.
#[macro_export]
macro_rules! as_blob {
    ($val:expr) => {{
        $crate::Value::Blob($val)
    }};
}

/// Constructs a new List Value from a list of one or more native data types.
///
/// # Examples
///
/// Write a list value to a record bin.
///
/// ```rust
/// # use aerospike::*;
/// # use std::vec::Vec;
/// # fn main() {
/// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap();
/// # let client = Client::new(&ClientPolicy::default(), &hosts).unwrap();
/// # let key = as_key!("test", "test", "mykey");
/// let list = as_list!("a", "b", "c");
/// let bin = as_bin!("list", list);
/// client.put(&WritePolicy::default(), &key, &vec![&bin]).unwrap();
/// # }
/// ```
#[macro_export]
macro_rules! as_list {
    ( $( $v:expr),* ) => {
        {
            let mut temp_vec = Vec::new();
            $(
                temp_vec.push(as_val!($v));
            )*
            $crate::Value::List(temp_vec)
        }
    };
}

/// Constructs a vector of Values from a list of one or more native data types.
///
/// # Examples
///
/// Execute a user-defined function (UDF) with some arguments.
///
/// ```rust,should_panic
/// # use aerospike::*;
/// # use std::vec::Vec;
/// # fn main() {
/// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap();
/// # let client = Client::new(&ClientPolicy::default(), &hosts).unwrap();
/// # let key = as_key!("test", "test", "mykey");
/// let module = "myUDF";
/// let func = "myFunction";
/// let args = as_values!("a", "b", "c");
/// client.execute_udf(&WritePolicy::default(), &key,
///     &module, &func, Some(&args)).unwrap();
/// # }
/// ```
#[macro_export]
macro_rules! as_values {
    ( $( $v:expr),* ) => {
        {
            let mut temp_vec = Vec::new();
            $(
                temp_vec.push(as_val!($v));
            )*
            temp_vec
        }
    };
}

/// Constructs a Map Value from a list of key/value pairs.
///
/// # Examples
///
/// Write a map value to a record bin.
///
/// ```rust
/// # use aerospike::*;
/// # fn main() {
/// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap();
/// # let client = Client::new(&ClientPolicy::default(), &hosts).unwrap();
/// # let key = as_key!("test", "test", "mykey");
/// let map = as_map!("a" => 1, "b" => 2);
/// let bin = as_bin!("map", map);
/// client.put(&WritePolicy::default(), &key, &vec![&bin]).unwrap();
/// # }
/// ```
#[macro_export]
macro_rules! as_map {
    ( $( $k:expr => $v:expr),* ) => {
        {
            let mut temp_map = std::collections::HashMap::new();
            $(
                temp_map.insert(as_val!($k), as_val!($v));
            )*
            $crate::Value::HashMap(temp_map)
        }
    };
}

#[cfg(feature = "serialization")]
impl Serialize for Value {
    fn serialize<S>(
        &self,
        serializer: S,
    ) -> std::result::Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        match &self {
            Value::Nil => serializer.serialize_none(),
            Value::Bool(b) => serializer.serialize_bool(*b),
            Value::Int(i) => serializer.serialize_i64(*i),
            Value::UInt(u) => serializer.serialize_u64(*u),
            Value::Float(f) => match f {
                FloatValue::F32(u) => serializer.serialize_u32(*u),
                FloatValue::F64(u) => serializer.serialize_u64(*u),
            },
            Value::String(s) | Value::GeoJSON(s) => serializer.serialize_str(s),
            Value::Blob(b) => serializer.serialize_bytes(&b[..]),
            Value::List(l) => {
                let mut seq = serializer.serialize_seq(Some(l.len()))?;
                for elem in l {
                    seq.serialize_element(&elem)?;
                }
                seq.end()
            }
            Value::HashMap(m) => {
                let mut map = serializer.serialize_map(Some(m.len()))?;
                for (key, value) in m {
                    map.serialize_entry(&key, &value)?;
                }
                map.end()
            }
            Value::OrderedMap(m) => {
                let mut map = serializer.serialize_map(Some(m.len()))?;
                for (key, value) in m {
                    map.serialize_entry(&key, &value)?;
                }
                map.end()
            }
            Value::HLL(b) => serializer.serialize_bytes(&b[..]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Value;

    #[test]
    fn as_string() {
        assert_eq!(Value::Nil.as_string(), String::from("<null>"));
        assert_eq!(Value::Int(42).as_string(), String::from("42"));
        assert_eq!(
            Value::UInt(9_223_372_036_854_775_808).as_string(),
            String::from("9223372036854775808")
        );
        assert_eq!(Value::Bool(true).as_string(), String::from("true"));
        assert_eq!(Value::from(4.1416).as_string(), String::from("4.1416"));
        assert_eq!(
            as_geo!(r#"{"type":"Point"}"#).as_string(),
            String::from(r#"{"type":"Point"}"#)
        );
    }

    #[test]
    fn as_geo() {
        let string = String::from(r#"{"type":"Point"}"#);
        let str = r#"{"type":"Point"}"#;
        assert_eq!(as_geo!(string), as_geo!(str));
    }

    #[test]
    #[cfg(feature = "serialization")]
    fn serializer() {
        let val: Value = as_list!("0", 9, 8, 7, 1, 2.1f64, -1, as_list!(5, 6, 7, 8, "asd"));
        let json = serde_json::to_string(&val);
        assert_eq!(
            json.unwrap(),
            "[\"0\",9,8,7,1,4611911198408756429,-1,[5,6,7,8,\"asd\"]]",
            "List Serialization failed"
        );

        let val: Value =
            as_map!("a" => 1, "b" => 2, "c" => 3, "d" => 4, "e" => 5, "f" => as_map!("test"=>123));
        let json = serde_json::to_string(&val);
        // We only check for the len of the String because HashMap serialization does not keep the key order. Comparing like the list above is not possible.
        assert_eq!(json.unwrap().len(), 48, "Map Serialization failed");
    }
}
