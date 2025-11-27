// Copyright (c) 2025 Contributors to the Eclipse Foundation
//
// See the NOTICE file(s) distributed with this work for additional
// information regarding copyright ownership.
//
// This program and the accompanying materials are made available under the
// terms of the Apache License Version 2.0 which is available at
// <https://www.apache.org/licenses/LICENSE-2.0>
//
// SPDX-License-Identifier: Apache-2.0

/// Key-value storage map type
pub type KvsMap = std::collections::HashMap<String, KvsValue>;

/// Key-value-storage value
#[derive(Clone, Debug, PartialEq)]
pub enum KvsValue {
    /// 32-bit signed integer
    I32(i32),

    /// 32-bit unsigned integer
    U32(u32),

    /// 64-bit signed integer
    I64(i64),

    /// 64-bit unsigned integer
    U64(u64),

    /// 64-bit float
    F64(f64),

    /// Boolean
    Boolean(bool),

    /// String
    String(String),

    /// Null
    Null,

    /// Array
    Array(Vec<KvsValue>),

    /// Object
    Object(KvsMap),
}

// Macro to implement From<T> for KvsValue for each supported type/variant.
// This allows concise and consistent conversion from basic Rust types to KvsValue.
macro_rules! impl_from_t_for_kvs_value {
    ($from:ty, $variant:ident) => {
        impl From<$from> for KvsValue {
            fn from(val: $from) -> Self {
                KvsValue::$variant(val)
            }
        }
    };
}

impl_from_t_for_kvs_value!(i32, I32);
impl_from_t_for_kvs_value!(u32, U32);
impl_from_t_for_kvs_value!(i64, I64);
impl_from_t_for_kvs_value!(u64, U64);
impl_from_t_for_kvs_value!(f64, F64);
impl_from_t_for_kvs_value!(bool, Boolean);
impl_from_t_for_kvs_value!(String, String);
impl_from_t_for_kvs_value!(Vec<KvsValue>, Array);
impl_from_t_for_kvs_value!(KvsMap, Object);

// Convert &str to KvsValue::String
impl From<&str> for KvsValue {
    fn from(val: &str) -> Self {
        KvsValue::String(val.to_string())
    }
}
// Convert unit type () to KvsValue::Null
impl From<()> for KvsValue {
    fn from(_: ()) -> Self {
        KvsValue::Null
    }
}

// Macro to implement TryFrom<&KvsValue> for T for each supported type/variant.
macro_rules! impl_tryfrom_kvs_value_to_t {
    ($to:ty, $variant:ident) => {
        impl core::convert::TryFrom<&KvsValue> for $to {
            type Error = String;
            fn try_from(value: &KvsValue) -> Result<Self, Self::Error> {
                if let KvsValue::$variant(ref n) = value {
                    Ok(n.clone())
                } else {
                    Err(format!("KvsValue is not a {}", stringify!($to)))
                }
            }
        }
    };
}

impl_tryfrom_kvs_value_to_t!(i32, I32);
impl_tryfrom_kvs_value_to_t!(u32, U32);
impl_tryfrom_kvs_value_to_t!(i64, I64);
impl_tryfrom_kvs_value_to_t!(u64, U64);
impl_tryfrom_kvs_value_to_t!(f64, F64);
impl_tryfrom_kvs_value_to_t!(bool, Boolean);
impl_tryfrom_kvs_value_to_t!(String, String);
impl_tryfrom_kvs_value_to_t!(Vec<KvsValue>, Array);
impl_tryfrom_kvs_value_to_t!(std::collections::HashMap<String, KvsValue>, Object);

impl TryFrom<&KvsValue> for () {
    type Error = &'static str;
    fn try_from(value: &KvsValue) -> Result<Self, Self::Error> {
        match value {
            KvsValue::Null => Ok(()),
            _ => Err("KvsValue is not a Null (unit type)"),
        }
    }
}

// Trait for extracting inner values from KvsValue
pub trait KvsValueGet {
    fn get_inner_value(val: &KvsValue) -> Option<&Self>;
}

impl KvsValue {
    pub fn get<T: KvsValueGet>(&self) -> Option<&T> {
        T::get_inner_value(self)
    }
}

macro_rules! impl_kvs_get_inner_value {
    ($to:ty, $variant:ident) => {
        impl KvsValueGet for $to {
            fn get_inner_value(v: &KvsValue) -> Option<&$to> {
                match v {
                    KvsValue::$variant(n) => Some(n),
                    _ => None,
                }
            }
        }
    };
}
impl_kvs_get_inner_value!(f64, F64);
impl_kvs_get_inner_value!(i32, I32);
impl_kvs_get_inner_value!(u32, U32);
impl_kvs_get_inner_value!(i64, I64);
impl_kvs_get_inner_value!(u64, U64);
impl_kvs_get_inner_value!(bool, Boolean);
impl_kvs_get_inner_value!(String, String);
impl_kvs_get_inner_value!(Vec<KvsValue>, Array);
impl_kvs_get_inner_value!(std::collections::HashMap<String, KvsValue>, Object);

impl KvsValueGet for () {
    fn get_inner_value(v: &KvsValue) -> Option<&()> {
        match v {
            KvsValue::Null => Some(&()),
            _ => None,
        }
    }
}
