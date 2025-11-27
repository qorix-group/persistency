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
use crate::kvs_api::{KvsApi, SnapshotId};
use crate::kvs_value::{KvsMap, KvsValue};

#[derive(Clone)]
pub struct MockKvs {
    pub map: std::sync::Arc<std::sync::Mutex<KvsMap>>,
    pub fail: bool,
}

impl Default for MockKvs {
    fn default() -> Self {
        let map = std::sync::Arc::new(std::sync::Mutex::new(KvsMap::new()));
        Self { map, fail: false }
    }
}

impl MockKvs {
    pub fn new(kvs_map: KvsMap, fail: bool) -> Result<Self, ErrorCode> {
        let map = std::sync::Arc::new(std::sync::Mutex::new(kvs_map));
        Ok(MockKvs { map, fail })
    }
}

impl KvsApi for MockKvs {
    fn reset(&self) -> Result<(), ErrorCode> {
        if self.fail {
            return Err(ErrorCode::UnmappedError);
        }
        self.map.lock().unwrap().clear();
        Ok(())
    }
    fn reset_key(&self, key: &str) -> Result<(), ErrorCode> {
        if self.fail {
            return Err(ErrorCode::UnmappedError);
        }
        let mut map = self.map.lock().unwrap();
        if map.contains_key(key) {
            map.remove(key);
            Ok(())
        } else {
            Err(ErrorCode::KeyDefaultNotFound)
        }
    }
    fn get_all_keys(&self) -> Result<Vec<String>, ErrorCode> {
        if self.fail {
            return Err(ErrorCode::UnmappedError);
        }
        Ok(self.map.lock().unwrap().keys().cloned().collect())
    }
    fn key_exists(&self, key: &str) -> Result<bool, ErrorCode> {
        if self.fail {
            return Err(ErrorCode::UnmappedError);
        }
        Ok(self.map.lock().unwrap().contains_key(key))
    }
    fn get_value(&self, key: &str) -> Result<KvsValue, ErrorCode> {
        if self.fail {
            return Err(ErrorCode::UnmappedError);
        }
        self.map
            .lock()
            .unwrap()
            .get(key)
            .cloned()
            .ok_or(ErrorCode::KeyNotFound)
    }
    fn get_value_as<T>(&self, key: &str) -> Result<T, ErrorCode>
    where
        for<'a> T: TryFrom<&'a KvsValue> + Clone,
        for<'a> <T as TryFrom<&'a KvsValue>>::Error: core::fmt::Debug,
    {
        if self.fail {
            return Err(ErrorCode::UnmappedError);
        }
        let v = self.get_value(key)?;
        T::try_from(&v).map_err(|_| ErrorCode::ConversionFailed)
    }
    fn get_default_value(&self, _key: &str) -> Result<KvsValue, ErrorCode> {
        if self.fail {
            return Err(ErrorCode::UnmappedError);
        }
        Err(ErrorCode::KeyNotFound)
    }
    fn is_value_default(&self, _key: &str) -> Result<bool, ErrorCode> {
        if self.fail {
            return Err(ErrorCode::UnmappedError);
        }
        Ok(false)
    }
    fn set_value<S: Into<String>, V: Into<KvsValue>>(
        &self,
        key: S,
        value: V,
    ) -> Result<(), ErrorCode> {
        if self.fail {
            return Err(ErrorCode::UnmappedError);
        }
        self.map.lock().unwrap().insert(key.into(), value.into());
        Ok(())
    }
    fn remove_key(&self, key: &str) -> Result<(), ErrorCode> {
        if self.fail {
            return Err(ErrorCode::UnmappedError);
        }
        self.map.lock().unwrap().remove(key);
        Ok(())
    }
    fn flush(&self) -> Result<(), ErrorCode> {
        if self.fail {
            return Err(ErrorCode::UnmappedError);
        }
        Ok(())
    }
    fn snapshot_count(&self) -> usize {
        if self.fail {
            return 9999;
        }
        0
    }
    fn snapshot_max_count(&self) -> usize {
        0
    }
    fn snapshot_restore(&self, _id: SnapshotId) -> Result<(), ErrorCode> {
        if self.fail {
            return Err(ErrorCode::UnmappedError);
        }
        Ok(())
    }
}
