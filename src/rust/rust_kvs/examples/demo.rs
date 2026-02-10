// *******************************************************************************
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
// *******************************************************************************
//! Demo application for Persistency module.
//! Performs the required sequence: create instance, store data, read data,
//! overwrite the data, restore snapshot, read data again.

use rust_kvs::prelude::*;
use std::collections::HashMap;
use tempfile::tempdir;

fn main() -> Result<(), ErrorCode> {
    // Temporary directory.
    let dir = tempdir()?;
    let dir_path = dir.path().to_path_buf();

    // Instance ID for KVS object instances.
    let instance_id = InstanceId(0);

    println!("=== Persistency Rust Demo ===");

    // 1. Create Persistency instance
    println!("1. Creating Persistency instance...");
    let builder = KvsBuilder::new(instance_id).backend(Box::new(
        JsonBackendBuilder::new()
            .working_dir(dir_path.clone())
            .build(),
    ));
    let kvs = builder.build()?;
    println!("   Instance created successfully");

    // 2. Store data
    println!("2. Storing initial data...");
    let key = "demo_key";
    let initial_value = "initial_value";
    kvs.set_value(key, initial_value)?;
    kvs.flush()?;
    println!("   Stored: {} = {}", key, initial_value);

    // 3. Read data
    println!("3. Reading data...");
    let read_value: String = kvs.get_value_as(key)?;
    println!("   Read: {} = {}", key, read_value);
    assert_eq!(read_value, initial_value);

    // 4. Overwrite the data
    println!("4. Overwriting data...");
    let new_value = "overwritten_value";
    kvs.set_value(key, new_value)?;
    kvs.flush()?;
    println!("   Overwritten: {} = {}", key, new_value);

    // 5. Restore snapshot
    println!("5. Restoring snapshot...");
    // Create a snapshot by flushing again
    kvs.flush()?;
    // Now restore to the previous snapshot (should have initial_value)
    kvs.snapshot_restore(SnapshotId(1))?;
    println!("   Restored to snapshot");

    // 6. Read data again
    println!("6. Reading data after restore...");
    let restored_value: String = kvs.get_value_as(key)?;
    println!("   Read after restore: {} = {}", key, restored_value);
    assert_eq!(restored_value, initial_value);

    println!("=== Demo completed successfully ===");
    Ok(())
}
