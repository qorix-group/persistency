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

#include "score/kvs/kvsbuilder.hpp"

#include "score/kvs/internal/kvs_instance_state.hpp"

namespace score::mw::per::kvs
{

KvsInstanceStateRegistry KvsBuilder::registry_{};

KvsBuilder::KvsBuilder(const InstanceId& instance_id) noexcept
    : instance_id_(instance_id),
      snapshot_id_(SnapshotId{0}),
      need_defaults_(OpenNeedDefaults::Optional),
      need_kvs_(OpenNeedKvs::Optional),
      directory_("./data_folder/"),
      maximum_storage_bytes_(std::nullopt),
      max_snapshots_(KVS_DEFAULT_MAX_SNAPSHOTS)
{
}

KvsBuilder& KvsBuilder::snapshot(const SnapshotId& snapshot_id) noexcept
{
    this->snapshot_id_ = snapshot_id;
    return *this;
}

KvsBuilder& KvsBuilder::need_defaults_flag(const OpenNeedDefaults& flag) noexcept
{
    need_defaults_ = flag;
    return *this;
}

KvsBuilder& KvsBuilder::need_kvs_flag(const OpenNeedKvs& flag) noexcept
{
    need_kvs_ = flag;
    return *this;
}

KvsBuilder& KvsBuilder::dir(const std::string& dir_path) noexcept
{
    this->directory_ = dir_path;
    return *this;
}

KvsBuilder& KvsBuilder::max_storage_bytes(const std::optional<size_t>& max_storage_bytes) noexcept
{
    this->maximum_storage_bytes_ = max_storage_bytes;
    return *this;
}

KvsBuilder& KvsBuilder::snapshot_max_count(const std::size_t max_count) noexcept
{
    this->max_snapshots_ = max_count;
    return *this;
}

void KvsBuilder::ResetRegistry() noexcept
{
    std::lock_guard<std::mutex> lock(registry_.mutex);

    for (std::size_t idx{0ul}; idx < registry_.size; ++idx)
    {
        registry_.array[idx].reset();
    }

    registry_.size = 0ul;
}

score::Result<Kvs> KvsBuilder::build() const noexcept
{
    // Use current directory if empty.
    const std::string directory{directory_.empty() ? std::string{"./"} : directory_};

    const std::size_t max_snapshots_clmaped{std::clamp(max_snapshots_, 0ul, KVS_MAX_SNAPSHOTS_UPPER_BOUND)};
    const score::filesystem::Path base_path(directory);
    const score::filesystem::Path filename_prefix = base_path / ("kvs_" + std::to_string(instance_id_.id));
    const score::filesystem::Path filename_default = filename_prefix.Native() + "_default";
    const score::filesystem::Path filename_kvs = filename_prefix.Native() + "_" + std::to_string(snapshot_id_.id);

    Kvs kvs;

    // Lock the registry.
    std::lock_guard<std::mutex> lock(registry_.mutex);

    // Check if already exists.
    for (std::size_t idx{0ul}; idx < registry_.size; ++idx)
    {
        const auto& entry{registry_.array[idx]};
        if (entry && entry->instance_id == instance_id_.id)
        {
            // Has it been created with different params?
            if (entry->snapshot_id != snapshot_id_.id || entry->max_snapshots != max_snapshots_clmaped ||
                entry->max_storage_bytes != maximum_storage_bytes_)
            {
                return score::MakeUnexpected(ErrorCode::InstanceParametersMismatch);
            }

            kvs.state_ = entry;
            return std::move(kvs);
        }
    }

    // No room for other instances.
    if (registry_.array.max_size() == registry_.size)
    {
        return score::MakeUnexpected(ErrorCode::ResourceBusy);
    }

    score::Result<KvsMap> default_values{};
    score::Result<KvsMap> current_values{};

    // Load the current values if requsted.
    if (need_defaults_ != OpenNeedDefaults::Ignored)
    {
        const auto need_file{need_defaults_ == OpenNeedDefaults::Required ? OpenJsonNeedFile::Required
                                                                          : OpenJsonNeedFile::Optional};
        default_values = kvs.open_json(filename_default, need_file);
        if (!default_values)
        {
            return score::MakeUnexpected(static_cast<ErrorCode>(*default_values.error()));
        }
    }

    // Load the default values if requested.
    if (need_kvs_ != OpenNeedKvs::Ignored)
    {
        const auto need_file{need_kvs_ == OpenNeedKvs::Required ? OpenJsonNeedFile::Required
                                                                : OpenJsonNeedFile::Optional};
        current_values = kvs.open_json(filename_kvs, need_file);
        if (!current_values)
        {
            return score::MakeUnexpected(static_cast<ErrorCode>(*current_values.error()));
        }
    }

    KvsInstanceStateRegistry::Entry entry{std::make_shared<KvsInstanceState>()};
    registry_.array[registry_.size++] = entry;

    entry->instance_id = instance_id_.id;
    entry->snapshot_id = snapshot_id_.id;
    entry->filename_prefix = std::move(filename_prefix);
    entry->current_values = std::move(current_values.value());
    entry->default_values = std::move(default_values.value());
    entry->max_snapshots = max_snapshots_clmaped;
    entry->max_storage_bytes = maximum_storage_bytes_;

    kvs.logger_->LogInfo() << "[instance=" << entry->instance_id << "]" << "[snapshot=" << entry->snapshot_id << "]"
                           << "[maxSnapshotCount=" << entry->max_snapshots << "]"
                           << "[maxStorageBytes=" << entry->max_storage_bytes.value_or(0ul) << "]" << " KVS created";

    kvs.state_ = entry;

    return std::move(kvs);
}

}  // namespace score::mw::per::kvs
