/********************************************************************************
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
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
#ifndef SCORE_LIB_KVS_INTERNAL_KVS_STATE_HPP
#define SCORE_LIB_KVS_INTERNAL_KVS_STATE_HPP

#include "score/filesystem/filesystem.h"
#include "score/kvs/kvsvalue.hpp"

#include <string>

namespace score::mw::per::kvs
{

/**
 * @brief Shared in-memory state for a KVS backing store.
 *
 * Handles opened for the same filename prefix share this state, including the
 * snapshot and storage limits. The storage limit applies to this store's files,
 * not to storage used by other KVS instances.
 */
struct KvsInstanceState final
{
    /// Protects access to the shared state.
    std::mutex mutex;
    /// Instance id that has been specified during creation.
    std::size_t instance_id;
    /// Snapshot id that has been specified during creation.
    std::size_t snapshot_id;
    /// Values in the current snapshot.
    KvsMap current_values;
    /// Default values loaded for this store.
    KvsMap default_values;
    /// Prefix used to construct this store's file paths.
    score::filesystem::Path filename_prefix;
    /// Configured maximum number of snapshots for this store.
    std::size_t max_snapshots;
    /// Optional total storage limit for this store's files.
    std::optional<size_t> max_storage_bytes;
};

} /* namespace score::mw::per::kvs */

#endif  // SCORE_LIB_KVS_INTERNAL_KVS_HELPER_HPP
