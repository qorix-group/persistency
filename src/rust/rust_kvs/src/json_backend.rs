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
use crate::kvs_api::{InstanceId, SnapshotId};
use crate::kvs_backend::KvsBackend;
use crate::kvs_value::{KvsMap, KvsValue};
use tinyjson::{JsonGenerateError, JsonParseError, JsonValue};

// Example of how KvsValue is stored in the JSON file (t-tagged format):
// {
//   "my_int": { "t": "i32", "v": 42 },
//   "my_float": { "t": "f64", "v": 3.1415 },
//   "my_bool": { "t": "bool", "v": true },
//   "my_string": { "t": "str", "v": "hello" },
//   "my_array": { "t": "arr", "v": [ ... ] },
//   "my_object": { "t": "obj", "v": { ... } },
//   "my_null": { "t": "null", "v": null }
// }

/// Backend-specific JsonValue -> KvsValue conversion.
impl From<JsonValue> for KvsValue {
    fn from(val: JsonValue) -> KvsValue {
        match val {
            JsonValue::Object(mut obj) => {
                // Type-tagged: { "t": ..., "v": ... }
                if let (Some(JsonValue::String(type_str)), Some(value)) =
                    (obj.remove("t"), obj.remove("v"))
                {
                    return match (type_str.as_str(), value) {
                        ("i32", JsonValue::Number(v)) => KvsValue::I32(v as i32),
                        ("u32", JsonValue::Number(v)) => KvsValue::U32(v as u32),
                        ("i64", JsonValue::Number(v)) => KvsValue::I64(v as i64),
                        ("u64", JsonValue::Number(v)) => KvsValue::U64(v as u64),
                        ("f64", JsonValue::Number(v)) => KvsValue::F64(v),
                        ("bool", JsonValue::Boolean(v)) => KvsValue::Boolean(v),
                        ("str", JsonValue::String(v)) => KvsValue::String(v),
                        ("null", JsonValue::Null) => KvsValue::Null,
                        ("arr", JsonValue::Array(v)) => {
                            KvsValue::Array(v.into_iter().map(KvsValue::from).collect())
                        }
                        ("obj", JsonValue::Object(v)) => KvsValue::Object(
                            v.into_iter().map(|(k, v)| (k, KvsValue::from(v))).collect(),
                        ),
                        // Remaining types can be handled with Null.
                        _ => KvsValue::Null,
                    };
                }
                // If not a t-tagged object, treat as a map of key-value pairs (KvsMap)
                let map: KvsMap = obj
                    .into_iter()
                    .map(|(k, v)| (k, KvsValue::from(v)))
                    .collect();
                KvsValue::Object(map)
            }
            // Remaining types can be handled with Null.
            _ => KvsValue::Null,
        }
    }
}

/// Backend-specific KvsValue -> JsonValue conversion.
impl From<KvsValue> for JsonValue {
    fn from(val: KvsValue) -> JsonValue {
        let mut obj = std::collections::HashMap::new();
        match val {
            KvsValue::I32(n) => {
                obj.insert("t".to_string(), JsonValue::String("i32".to_string()));
                obj.insert("v".to_string(), JsonValue::Number(n as f64));
            }
            KvsValue::U32(n) => {
                obj.insert("t".to_string(), JsonValue::String("u32".to_string()));
                obj.insert("v".to_string(), JsonValue::Number(n as f64));
            }
            KvsValue::I64(n) => {
                obj.insert("t".to_string(), JsonValue::String("i64".to_string()));
                obj.insert("v".to_string(), JsonValue::Number(n as f64));
            }
            KvsValue::U64(n) => {
                obj.insert("t".to_string(), JsonValue::String("u64".to_string()));
                obj.insert("v".to_string(), JsonValue::Number(n as f64));
            }
            KvsValue::F64(n) => {
                obj.insert("t".to_string(), JsonValue::String("f64".to_string()));
                obj.insert("v".to_string(), JsonValue::Number(n));
            }
            KvsValue::Boolean(b) => {
                obj.insert("t".to_string(), JsonValue::String("bool".to_string()));
                obj.insert("v".to_string(), JsonValue::Boolean(b));
            }
            KvsValue::String(s) => {
                obj.insert("t".to_string(), JsonValue::String("str".to_string()));
                obj.insert("v".to_string(), JsonValue::String(s));
            }
            KvsValue::Null => {
                obj.insert("t".to_string(), JsonValue::String("null".to_string()));
                obj.insert("v".to_string(), JsonValue::Null);
            }
            KvsValue::Array(arr) => {
                obj.insert("t".to_string(), JsonValue::String("arr".to_string()));
                obj.insert(
                    "v".to_string(),
                    JsonValue::Array(arr.into_iter().map(JsonValue::from).collect()),
                );
            }
            KvsValue::Object(map) => {
                obj.insert("t".to_string(), JsonValue::String("obj".to_string()));
                obj.insert(
                    "v".to_string(),
                    JsonValue::Object(
                        map.into_iter()
                            .map(|(k, v)| (k, JsonValue::from(v)))
                            .collect(),
                    ),
                );
            }
        }
        JsonValue::Object(obj)
    }
}

/// tinyjson::JsonParseError -> ErrorCode::JsonParseError
impl From<JsonParseError> for ErrorCode {
    fn from(cause: JsonParseError) -> Self {
        eprintln!(
            "error: JSON parser error: line = {}, column = {}",
            cause.line(),
            cause.column()
        );
        ErrorCode::JsonParserError
    }
}

/// tinyjson::JsonGenerateError -> ErrorCode::JsonGenerateError
impl From<JsonGenerateError> for ErrorCode {
    fn from(cause: JsonGenerateError) -> Self {
        eprintln!("error: JSON generator error: msg = {}", cause.message());
        ErrorCode::JsonGeneratorError
    }
}

/// Builder for `JsonBackend`.
pub struct JsonBackendBuilder {
    working_dir: std::path::PathBuf,
    snapshot_max_count: usize,
}

impl JsonBackendBuilder {
    pub fn new() -> Self {
        Self {
            working_dir: std::path::PathBuf::new(),
            snapshot_max_count: 3,
        }
    }

    pub fn working_dir(mut self, working_dir: std::path::PathBuf) -> Self {
        self.working_dir = working_dir;
        self
    }

    pub fn snapshot_max_count(mut self, snapshot_max_count: usize) -> Self {
        self.snapshot_max_count = snapshot_max_count;
        self
    }

    pub fn build(self) -> JsonBackend {
        JsonBackend {
            working_dir: self.working_dir,
            snapshot_max_count: self.snapshot_max_count,
        }
    }
}

impl Default for JsonBackendBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// KVS backend implementation based on TinyJSON.
#[derive(Clone, PartialEq)]
pub struct JsonBackend {
    working_dir: std::path::PathBuf,
    snapshot_max_count: usize,
}

impl JsonBackend {
    fn parse(s: &str) -> Result<JsonValue, ErrorCode> {
        s.parse().map_err(ErrorCode::from)
    }

    fn stringify(val: &JsonValue) -> Result<String, ErrorCode> {
        val.stringify().map_err(ErrorCode::from)
    }

    /// Rotate snapshots
    ///
    /// # Features
    ///   * `FEAT_REQ__KVS__snapshots`
    ///
    /// # Return Values
    ///   * Ok: Rotation successful, also if no rotation was needed
    ///   * `ErrorCode::UnmappedError`: Unmapped error
    fn snapshot_rotate(&self, instance_id: InstanceId) -> Result<(), ErrorCode> {
        for idx in (1..self.snapshot_max_count()).rev() {
            let old_snapshot_id = SnapshotId(idx - 1);
            let new_snapshot_id = SnapshotId(idx);

            let hash_path_old = self.hash_file_path(instance_id, old_snapshot_id);
            let hash_path_new = self.hash_file_path(instance_id, new_snapshot_id);
            let snap_name_old = Self::kvs_file_name(instance_id, old_snapshot_id);
            let snap_path_old = self.kvs_file_path(instance_id, old_snapshot_id);
            let snap_name_new = Self::kvs_file_name(instance_id, new_snapshot_id);
            let snap_path_new = self.kvs_file_path(instance_id, new_snapshot_id);

            println!("rotating: {snap_name_old} -> {snap_name_new}");

            // Check snapshot and hash files exist.
            let snap_old_exists = snap_path_old.exists();
            let hash_old_exists = hash_path_old.exists();

            // If both exist - rename them.
            if snap_old_exists && hash_old_exists {
                std::fs::rename(hash_path_old, hash_path_new)?;
                std::fs::rename(snap_path_old, snap_path_new)?;
            }
            // If neither exist - continue.
            else if !snap_old_exists && !hash_old_exists {
                continue;
            }
            // In other case - this is erroneous scenario.
            // Either snapshot or hash file got removed.
            else {
                return Err(ErrorCode::IntegrityCorrupted);
            }
        }

        Ok(())
    }

    /// Check path have correct extension.
    fn check_extension(path: &std::path::Path, extension: &str) -> bool {
        let ext = path.extension();
        ext.is_some_and(|ep| ep.to_str().is_some_and(|es| es == extension))
    }

    pub(super) fn load(kvs_path: &std::path::Path, hash_path: &std::path::Path) -> Result<KvsMap, ErrorCode> {
        if !Self::check_extension(kvs_path, "json") {
            return Err(ErrorCode::KvsFileReadError);
        }
        if !Self::check_extension(hash_path, "hash") {
            return Err(ErrorCode::KvsHashFileReadError);
        }

        // Load KVS file and parse from string to `JsonValue`.
        let json_str = std::fs::read_to_string(kvs_path)?;
        let json_value = Self::parse(&json_str)?;

        // Perform hash check.
        match std::fs::read(hash_path) {
            Ok(hash_bytes) => {
                let hash_kvs = adler32::RollingAdler32::from_buffer(json_str.as_bytes()).hash();
                if hash_bytes.len() == 4 {
                    let file_hash = u32::from_be_bytes([
                        hash_bytes[0],
                        hash_bytes[1],
                        hash_bytes[2],
                        hash_bytes[3],
                    ]);
                    if hash_kvs != file_hash {
                        return Err(ErrorCode::ValidationFailed);
                    }
                } else {
                    return Err(ErrorCode::ValidationFailed);
                }
            }
            Err(e) => return Err(e.into()),
        };

        // Cast from `JsonValue` to `KvsValue`.
        let kvs_value = KvsValue::from(json_value);
        if let KvsValue::Object(kvs_map) = kvs_value {
            Ok(kvs_map)
        } else {
            Err(ErrorCode::JsonParserError)
        }
    }

    pub(super) fn save(
        kvs_map: &KvsMap,
        kvs_path: &std::path::Path,
        hash_path: &std::path::Path,
    ) -> Result<(), ErrorCode> {
        // Validate extensions.
        if !Self::check_extension(kvs_path, "json") {
            return Err(ErrorCode::KvsFileReadError);
        }
        if !Self::check_extension(hash_path, "hash") {
            return Err(ErrorCode::KvsHashFileReadError);
        }

        // Cast from `KvsValue` to `JsonValue`.
        let kvs_value = KvsValue::Object(kvs_map.clone());
        let json_value = JsonValue::from(kvs_value);

        // Stringify `JsonValue` and save to KVS file.
        let json_str = Self::stringify(&json_value)?;
        std::fs::write(kvs_path, &json_str)?;

        // Generate hash and save to hash file.
        let hash = adler32::RollingAdler32::from_buffer(json_str.as_bytes()).hash();
        std::fs::write(hash_path, hash.to_be_bytes())?;

        Ok(())
    }

    /// Get KVS file name.
    pub fn kvs_file_name(instance_id: InstanceId, snapshot_id: SnapshotId) -> String {
        format!("kvs_{instance_id}_{snapshot_id}.json")
    }

    /// Get KVS file path in working directory.
    pub fn kvs_file_path(&self, instance_id: InstanceId, snapshot_id: SnapshotId) -> std::path::PathBuf {
        self.working_dir
            .join(Self::kvs_file_name(instance_id, snapshot_id))
    }

    /// Get hash file name.
    pub fn hash_file_name(instance_id: InstanceId, snapshot_id: SnapshotId) -> String {
        format!("kvs_{instance_id}_{snapshot_id}.hash")
    }

    /// Get hash file path in working directory.
    pub fn hash_file_path(&self, instance_id: InstanceId, snapshot_id: SnapshotId) -> std::path::PathBuf {
        self.working_dir
            .join(Self::hash_file_name(instance_id, snapshot_id))
    }

    /// Get defaults file name.
    pub fn defaults_file_name(instance_id: InstanceId) -> String {
        format!("kvs_{instance_id}_default.json")
    }

    /// Get defaults file path in working directory.
    pub fn defaults_file_path(&self, instance_id: InstanceId) -> std::path::PathBuf {
        self.working_dir.join(Self::defaults_file_name(instance_id))
    }

    /// Get defaults hash file name.
    pub fn defaults_hash_file_name(instance_id: InstanceId) -> String {
        format!("kvs_{instance_id}_default.hash")
    }

    /// Get defaults hash file path in working directory.
    pub fn defaults_hash_file_path(&self, instance_id: InstanceId) -> std::path::PathBuf {
        self.working_dir
            .join(Self::defaults_hash_file_name(instance_id))
    }
}

impl KvsBackend for JsonBackend {
    fn load_kvs(
        &self,
        instance_id: InstanceId,
        snapshot_id: SnapshotId,
    ) -> Result<KvsMap, ErrorCode> {
        let kvs_path = self.kvs_file_path(instance_id, snapshot_id);
        let hash_path = self.hash_file_path(instance_id, snapshot_id);
        Self::load(&kvs_path, &hash_path)
    }

    fn load_defaults(&self, instance_id: InstanceId) -> Result<KvsMap, ErrorCode> {
        let defaults_path = self.defaults_file_path(instance_id);
        let defaults_hash_path = self.defaults_hash_file_path(instance_id);
        Self::load(&defaults_path, &defaults_hash_path)
    }

    fn flush(&self, instance_id: InstanceId, kvs_map: &KvsMap) -> Result<(), ErrorCode> {
        self.snapshot_rotate(instance_id).map_err(|e| {
            eprintln!("error: snapshot_rotate failed: {e:?}");
            e
        })?;
        let snapshot_id = SnapshotId(0);
        let kvs_path = self.kvs_file_path(instance_id, snapshot_id);
        let hash_path = self.hash_file_path(instance_id, snapshot_id);
        Self::save(kvs_map, &kvs_path, &hash_path).map_err(|e| {
            eprintln!("error: save failed: {e:?}");
            e
        })?;
        Ok(())
    }

    fn snapshot_count(&self, instance_id: InstanceId) -> usize {
        let mut count = 0;

        for idx in 0..self.snapshot_max_count {
            let snapshot_id = SnapshotId(idx);
            let snapshot_path = self.kvs_file_path(instance_id, snapshot_id);
            if !snapshot_path.exists() {
                break;
            }

            count += 1;
        }

        count
    }

    fn snapshot_max_count(&self) -> usize {
        self.snapshot_max_count
    }

    fn snapshot_restore(
        &self,
        instance_id: InstanceId,
        snapshot_id: SnapshotId,
    ) -> Result<KvsMap, ErrorCode> {
        // fail if the snapshot ID is the current KVS
        if snapshot_id == SnapshotId(0) {
            eprintln!("error: tried to restore current KVS as snapshot");
            return Err(ErrorCode::InvalidSnapshotId);
        }

        if self.snapshot_count(instance_id) < snapshot_id.0 {
            eprintln!("error: tried to restore a non-existing snapshot");
            return Err(ErrorCode::InvalidSnapshotId);
        }

        self.load_kvs(instance_id, snapshot_id)
    }
}
