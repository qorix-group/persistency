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
use crate::json_backend::JsonBackendBuilder;
use crate::kvs::{Kvs, KvsParameters};
use crate::kvs_api::{InstanceId, KvsDefaults, KvsLoad, SnapshotId};
use crate::kvs_backend::KvsBackend;
use crate::kvs_value::KvsMap;

/// Maximum number of instances.
const KVS_MAX_INSTANCES: usize = 10;

/// KVS instance data.
/// Expected to be shared between instance pool and instances.
pub(crate) struct KvsData {
    /// Storage data.
    pub(crate) kvs_map: KvsMap,

    /// Optional default values.
    pub(crate) defaults_map: KvsMap,
}

impl From<std::sync::PoisonError<std::sync::MutexGuard<'_, KvsData>>> for ErrorCode {
    fn from(_cause: std::sync::PoisonError<std::sync::MutexGuard<'_, KvsData>>) -> Self {
        ErrorCode::MutexLockFailed
    }
}

/// KVS instance inner representation.
pub(crate) struct KvsInner {
    /// KVS instance parameters.
    pub(crate) parameters: std::sync::Arc<KvsParameters>,

    /// KVS instance data.
    pub(crate) data: std::sync::Arc<std::sync::Mutex<KvsData>>,
}

static KVS_POOL: std::sync::LazyLock<std::sync::Mutex<[Option<KvsInner>; KVS_MAX_INSTANCES]>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new([const { None }; KVS_MAX_INSTANCES]));

impl From<std::sync::PoisonError<std::sync::MutexGuard<'_, [Option<KvsInner>; KVS_MAX_INSTANCES]>>> for ErrorCode {
    fn from(_cause: std::sync::PoisonError<std::sync::MutexGuard<'_, [Option<KvsInner>; KVS_MAX_INSTANCES]>>) -> Self {
        ErrorCode::MutexLockFailed
    }
}

/// Key-value-storage builder.
pub struct KvsBuilder {
    /// Instance ID.
    instance_id: InstanceId,

    /// Defaults handling mode.
    defaults: Option<KvsDefaults>,

    /// KVS load mode.
    kvs_load: Option<KvsLoad>,

    /// Backend.
    backend: Option<Box<dyn KvsBackend>>,
}

impl KvsBuilder {
    /// Create a builder to open the key-value-storage
    ///
    /// Only the instance ID must be set. All other settings are using default values until changed
    /// via the builder API.
    ///
    /// # Parameters
    ///   * `instance_id`: Instance ID
    ///
    /// # Return Values
    ///   * KvsBuilder instance
    pub fn new(instance_id: InstanceId) -> Self {
        Self {
            instance_id,
            defaults: None,
            kvs_load: None,
            backend: None,
        }
    }

    /// Return maximum number of allowed KVS instances.
    ///
    /// # Return Values
    ///   * Max number of KVS instances
    pub fn max_instances() -> usize {
        KVS_MAX_INSTANCES
    }

    /// Configure defaults handling mode.
    ///
    /// # Parameters
    ///   * `mode`: defaults handling mode (default: [`KvsDefaults::Optional`](KvsDefaults::Optional))
    ///
    /// # Return Values
    ///   * KvsBuilder instance
    pub fn defaults(mut self, mode: KvsDefaults) -> Self {
        self.defaults = Some(mode);
        self
    }

    /// Configure KVS load mode.
    ///
    /// # Parameters
    ///   * `mode`: KVS load mode (default: [`KvsLoad::Optional`](KvsLoad::Optional))
    ///
    /// # Return Values
    ///   * KvsBuilder instance
    pub fn kvs_load(mut self, mode: KvsLoad) -> Self {
        self.kvs_load = Some(mode);
        self
    }

    /// Set backend.
    ///
    /// # Parameters
    ///   * `backend`: KVS backend.
    ///
    /// # Return Values
    ///   * KvsBuilder instance
    pub fn backend(mut self, backend: Box<dyn KvsBackend>) -> Self {
        self.backend = Some(backend);
        self
    }

    /// Compare existing parameters with expected configuration.
    fn compare_parameters(&self, other: &KvsParameters) -> bool {
        // Compare instance ID.
        if self.instance_id != other.instance_id {
            eprintln!("error: instance ID mismatched");
            false
        }
        // Compare defaults handling mode.
        else if self.defaults.is_some_and(|v| v != other.defaults) {
            eprintln!("error: defaults handling mode mismatched");
            false
        }
        // Compare KVS load mode.
        else if self.kvs_load.is_some_and(|v| v != other.kvs_load) {
            eprintln!("error: KVS load mode mismatched");
            false
        }
        // Compare backend.
        else if self
            .backend
            .as_ref()
            .is_some_and(|v| !v.dyn_eq(other.backend.as_any()))
        {
            eprintln!("error: backend parameters mismatched");
            false
        }
        // Success.
        else {
            true
        }
    }

    /// Finalize the builder and open the key-value-storage
    ///
    /// Calls `Kvs::open` with the configured settings.
    ///
    /// # Features
    ///   * `FEAT_REQ__KVS__default_values`
    ///   * `FEAT_REQ__KVS__multiple_kvs`
    ///   * `FEAT_REQ__KVS__integrity_check`
    ///
    /// # Return Values
    ///   * Ok: KVS instance
    ///   * `ErrorCode::ValidationFailed`: KVS hash validation failed
    ///   * `ErrorCode::JsonParserError`: JSON parser error
    ///   * `ErrorCode::KvsFileReadError`: KVS file read error
    ///   * `ErrorCode::KvsHashFileReadError`: KVS hash file read error
    ///   * `ErrorCode::UnmappedError`: Generic error
    pub fn build(self) -> Result<Kvs, ErrorCode> {
        let instance_id = self.instance_id;
        let instance_id_index: usize = instance_id.into();

        // Check if instance already exists.
        {
            let kvs_pool = KVS_POOL.lock()?;
            let kvs_inner_option = match kvs_pool.get(instance_id_index) {
                Some(kvs_pool_entry) => match kvs_pool_entry {
                    // If instance exists then parameters must match.
                    Some(kvs_inner) => {
                        if self.compare_parameters(&kvs_inner.parameters) {
                            Ok(Some(kvs_inner))
                        } else {
                            Err(ErrorCode::InstanceParametersMismatch)
                        }
                    }
                    // Instance not found - not an error, will initialize later.
                    None => Ok(None),
                },
                // Instance ID out of range.
                None => Err(ErrorCode::InvalidInstanceId),
            }?;

            // Return existing instance if initialized.
            if let Some(kvs_inner) = kvs_inner_option {
                return Ok(Kvs::new(
                    kvs_inner.data.clone(),
                    kvs_inner.parameters.clone(),
                ));
            }
        }

        // Initialize KVS instance with provided parameters.
        let parameters = {
            let defaults = match self.defaults {
                Some(defaults) => defaults,
                None => KvsDefaults::Optional,
            };

            let kvs_load = match self.kvs_load {
                Some(kvs_load) => kvs_load,
                None => KvsLoad::Optional,
            };

            let backend = match self.backend {
                Some(backend) => backend,
                None => Box::new(JsonBackendBuilder::new().build()),
            };

            KvsParameters {
                instance_id,
                defaults,
                kvs_load,
                backend,
            }
        };

        // Load defaults.
        let defaults_map = match parameters.defaults {
            KvsDefaults::Ignored => KvsMap::new(),
            KvsDefaults::Optional => match parameters.backend.load_defaults(instance_id) {
                Ok(map) => map,
                Err(e) => match e {
                    ErrorCode::FileNotFound => KvsMap::new(),
                    _ => return Err(e),
                },
            },
            KvsDefaults::Required => parameters.backend.load_defaults(instance_id)?,
        };

        // Load KVS and hash files.
        let snapshot_id = SnapshotId(0);
        let kvs_map = match parameters.kvs_load {
            KvsLoad::Ignored => KvsMap::new(),
            KvsLoad::Optional => match parameters.backend.load_kvs(instance_id, snapshot_id) {
                Ok(map) => map,
                Err(e) => match e {
                    ErrorCode::FileNotFound => KvsMap::new(),
                    _ => return Err(e),
                },
            },
            KvsLoad::Required => parameters.backend.load_kvs(instance_id, snapshot_id)?,
        };

        // Shared object containing data.
        let data = std::sync::Arc::new(std::sync::Mutex::new(KvsData {
            kvs_map,
            defaults_map,
        }));

        // Shared object containing parameters.
        let parameters = std::sync::Arc::new(parameters);

        // Initialize entry in pool and return new KVS instance.
        {
            let mut kvs_pool = KVS_POOL.lock()?;
            let kvs_pool_entry = match kvs_pool.get_mut(instance_id_index) {
                Some(entry) => entry,
                None => return Err(ErrorCode::InvalidInstanceId),
            };

            let _ = kvs_pool_entry.insert(KvsInner {
                parameters: parameters.clone(),
                data: data.clone(),
            });
        }

        Ok(Kvs::new(data, parameters))
    }
}
