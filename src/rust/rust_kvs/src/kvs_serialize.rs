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

use crate::error_code::ErrorCode;
use crate::kvs_value::{KvsMap, KvsValue};

/// `KvsValue` serialization trait.
/// Allows object to be serialized into `KvsValue`.
pub trait KvsSerialize {
    type Error;

    /// Serialize object to `KvsValue`.
    fn to_kvs(&self) -> Result<KvsValue, Self::Error>;
}

macro_rules! impl_kvs_serialize_for_t_unchecked_cast {
    ($t:ty, $internal_t:ty, $variant:ident) => {
        impl KvsSerialize for $t {
            type Error = ErrorCode;

            fn to_kvs(&self) -> Result<KvsValue, Self::Error> {
                Ok(KvsValue::$variant(self.clone() as $internal_t))
            }
        }
    };
}

macro_rules! impl_kvs_serialize_for_t_checked_cast {
    ($t:ty, $internal_t:ty, $variant:ident) => {
        impl KvsSerialize for $t {
            type Error = ErrorCode;

            fn to_kvs(&self) -> Result<KvsValue, Self::Error> {
                if let Ok(casted) = <$internal_t>::try_from(self.clone()) {
                    Ok(KvsValue::$variant(casted))
                } else {
                    Err(ErrorCode::SerializationFailed(
                        "Value to KvsValue cast failed".to_string(),
                    ))
                }
            }
        }
    };
}

macro_rules! impl_kvs_serialize_for_t {
    ($t:ty, $variant:ident) => {
        impl KvsSerialize for $t {
            type Error = ErrorCode;

            fn to_kvs(&self) -> Result<KvsValue, Self::Error> {
                Ok(KvsValue::$variant(self.clone()))
            }
        }
    };
}

impl_kvs_serialize_for_t_unchecked_cast!(i8, i32, I32);
impl_kvs_serialize_for_t_unchecked_cast!(i16, i32, I32);
impl_kvs_serialize_for_t!(i32, I32);
impl_kvs_serialize_for_t!(i64, I64);
impl_kvs_serialize_for_t_checked_cast!(isize, i64, I64);
impl_kvs_serialize_for_t_unchecked_cast!(u8, u32, U32);
impl_kvs_serialize_for_t_unchecked_cast!(u16, u32, U32);
impl_kvs_serialize_for_t!(u32, U32);
impl_kvs_serialize_for_t!(u64, U64);
impl_kvs_serialize_for_t_checked_cast!(usize, u64, U64);
impl_kvs_serialize_for_t_unchecked_cast!(f32, f64, F64);
impl_kvs_serialize_for_t!(f64, F64);
impl_kvs_serialize_for_t!(bool, Boolean);
impl_kvs_serialize_for_t!(String, String);
impl_kvs_serialize_for_t!(Vec<KvsValue>, Array);
impl_kvs_serialize_for_t!(KvsMap, Object);

impl KvsSerialize for &str {
    type Error = ErrorCode;

    fn to_kvs(&self) -> Result<KvsValue, Self::Error> {
        Ok(KvsValue::String(self.to_string()))
    }
}

impl KvsSerialize for () {
    type Error = ErrorCode;

    fn to_kvs(&self) -> Result<KvsValue, Self::Error> {
        Ok(KvsValue::Null)
    }
}

/// `KvsValue` deserialization trait.
/// Allows object to be deserialized from `KvsValue`.
pub trait KvsDeserialize: Sized {
    type Error;

    /// Deserialize object from `KvsValue`.
    fn from_kvs(kvs_value: &KvsValue) -> Result<Self, Self::Error>;
}

macro_rules! impl_kvs_deserialize_for_t_checked_cast {
    ($t:ty, $variant:ident) => {
        impl KvsDeserialize for $t {
            type Error = ErrorCode;

            fn from_kvs(kvs_value: &KvsValue) -> Result<Self, Self::Error> {
                if let KvsValue::$variant(value) = kvs_value {
                    if let Ok(casted) = <$t>::try_from(value.clone()) {
                        Ok(casted)
                    } else {
                        Err(ErrorCode::DeserializationFailed(
                            "KvsValue to value cast failed".to_string(),
                        ))
                    }
                } else {
                    Err(ErrorCode::DeserializationFailed(
                        "Invalid KvsValue variant provided".to_string(),
                    ))
                }
            }
        }
    };
}

macro_rules! impl_kvs_deserialize_for_t {
    ($t:ty, $variant:ident) => {
        impl KvsDeserialize for $t {
            type Error = ErrorCode;

            fn from_kvs(kvs_value: &KvsValue) -> Result<Self, Self::Error> {
                if let KvsValue::$variant(value) = kvs_value {
                    Ok(value.clone())
                } else {
                    Err(ErrorCode::DeserializationFailed(
                        "Invalid KvsValue variant provided".to_string(),
                    ))
                }
            }
        }
    };
}

impl_kvs_deserialize_for_t_checked_cast!(i8, I32);
impl_kvs_deserialize_for_t_checked_cast!(i16, I32);
impl_kvs_deserialize_for_t!(i32, I32);
impl_kvs_deserialize_for_t!(i64, I64);
impl_kvs_deserialize_for_t_checked_cast!(isize, I64);
impl_kvs_deserialize_for_t_checked_cast!(u8, U32);
impl_kvs_deserialize_for_t_checked_cast!(u16, U32);
impl_kvs_deserialize_for_t!(u32, U32);
impl_kvs_deserialize_for_t!(u64, U64);
impl_kvs_deserialize_for_t_checked_cast!(usize, U64);
impl_kvs_deserialize_for_t!(f64, F64);
impl_kvs_deserialize_for_t!(bool, Boolean);
impl_kvs_deserialize_for_t!(String, String);
impl_kvs_deserialize_for_t!(Vec<KvsValue>, Array);
impl_kvs_deserialize_for_t!(KvsMap, Object);

/// Edge case - `TryFrom` is not implemented for `f32`.
/// Unchecked `as` conversion must be used.
impl KvsDeserialize for f32 {
    type Error = ErrorCode;

    fn from_kvs(kvs_value: &KvsValue) -> Result<Self, Self::Error> {
        if let KvsValue::F64(value) = kvs_value {
            Ok(*value as f32)
        } else {
            Err(ErrorCode::DeserializationFailed(
                "Invalid KvsValue variant provided".to_string(),
            ))
        }
    }
}

impl KvsDeserialize for () {
    type Error = ErrorCode;

    fn from_kvs(kvs_value: &KvsValue) -> Result<Self, Self::Error> {
        if let KvsValue::Null = kvs_value {
            Ok(())
        } else {
            Err(ErrorCode::DeserializationFailed(
                "Invalid KvsValue variant provided".to_string(),
            ))
        }
    }
}
