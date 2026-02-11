/********************************************************************************
 * Copyright (c) 2025 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Apache License Version 2.0 which is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * SPDX-License-Identifier: Apache-2.0
 ********************************************************************************/
//! Demo application for Persistency module.
//! Performs the required sequence: create instance, store data, read data,
//! overwrite the data, restore snapshot, read data again.

#include "kvs.hpp"
#include "kvsbuilder.hpp"
#include <iostream>
#include <string>
#include <filesystem>

int main() {
    std::cout << "=== Persistency C++ Demo ===" << std::endl;

    // Temporary directory
    std::filesystem::path temp_dir = std::filesystem::temp_directory_path() / "kvs_demo";
    std::filesystem::create_directories(temp_dir);

    // Instance ID
    score::mw::per::kvs::InstanceId instance_id(0);

    try {
        // 1. Create Persistency instance
        std::cout << "1. Creating Persistency instance..." << std::endl;
        auto kvs_result = score::mw::per::kvs::KvsBuilder(instance_id)
            .need_defaults_flag(false)
            .need_kvs_flag(false)
            .dir(temp_dir.string())
            .build();
        if (!kvs_result) {
            std::cerr << "Failed to create KVS instance" << std::endl;
            return 1;
        }
        auto kvs = std::move(kvs_result.value());
        std::cout << "   Instance created successfully" << std::endl;

        // 2. Store data
        std::cout << "2. Storing initial data..." << std::endl;
        std::string key = "demo_key";
        std::string initial_value = "initial_value";
        auto set_result = kvs.set_value(key, score::mw::per::kvs::KvsValue(initial_value));
        if (!set_result) {
            std::cerr << "Failed to set value" << std::endl;
            return 1;
        }
        auto flush_result = kvs.flush();
        if (!flush_result) {
            std::cerr << "Failed to flush" << std::endl;
            return 1;
        }
        std::cout << "   Stored: " << key << " = " << initial_value << std::endl;

        // 3. Read data
        std::cout << "3. Reading data..." << std::endl;
        auto get_result = kvs.get_value(key);
        if (!get_result) {
            std::cerr << "Failed to get value" << std::endl;
            return 1;
        }
        if (get_result.value().getType() != score::mw::per::kvs::KvsValue::Type::String) {
            std::cerr << "Read value is not a string" << std::endl;
            return 1;
        }
        auto read_value = std::get<std::string>(get_result.value().getValue());
        std::cout << "   Read: " << key << " = " << read_value << std::endl;
        if (read_value != initial_value) {
            std::cerr << "Value mismatch!" << std::endl;
            return 1;
        }

        // 4. Overwrite the data
        std::cout << "4. Overwriting data..." << std::endl;
        std::string new_value = "overwritten_value";
        auto set_new_result = kvs.set_value(key, score::mw::per::kvs::KvsValue(new_value));
        if (!set_new_result) {
            std::cerr << "Failed to set new value" << std::endl;
            return 1;
        }
        auto flush_new_result = kvs.flush();
        if (!flush_new_result) {
            std::cerr << "Failed to flush new value" << std::endl;
            return 1;
        }
        std::cout << "   Overwritten: " << key << " = " << new_value << std::endl;

        // 5. Restore snapshot
        std::cout << "5. Restoring snapshot..." << std::endl;
        // Create a snapshot by flushing again
        auto flush_snapshot_result = kvs.flush();
        if (!flush_snapshot_result) {
            std::cerr << "Failed to create snapshot" << std::endl;
            return 1;
        }
        // Restore to previous snapshot
        auto restore_result = kvs.snapshot_restore(score::mw::per::kvs::SnapshotId(0));
        if (!restore_result) {
            std::cerr << "Failed to restore snapshot" << std::endl;
            return 1;
        }
        std::cout << "   Restored to snapshot" << std::endl;

        // 6. Read data again
        std::cout << "6. Reading data after restore..." << std::endl;
        auto get_restored_result = kvs.get_value(key);
        if (!get_restored_result) {
            std::cerr << "Failed to get restored value" << std::endl;
            return 1;
        }
        if (get_restored_result.value().getType() != score::mw::per::kvs::KvsValue::Type::String) {
            std::cerr << "Restored value is not a string" << std::endl;
            return 1;
        }
        auto restored_value = std::get<std::string>(get_restored_result.value().getValue());
        std::cout << "   Read after restore: " << key << " = " << restored_value << std::endl;
        if (restored_value != initial_value) {
            std::cerr << "Restored value mismatch!" << std::endl;
            return 1;
        }

        std::cout << "=== Demo completed successfully ===" << std::endl;

        // Cleanup
        std::filesystem::remove_all(temp_dir);

    } catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
