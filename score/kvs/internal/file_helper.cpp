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

#include "score/kvs/internal/file_helper.h"
#include "score/assert.hpp"
#include "score/kvs/error.h"
#include "score/kvs/internal/kvs_helper.h"
#include "score/os/fcntl.h"
#include "score/os/unistd.h"
#include "score/scope_exit/scope_exit.h"
#include <cstdint>

using namespace score::kvs;

namespace
{
/// @brief Magic number for file format recognition.
/// Contains `SKVS` in ASCII as hex.
const uint32_t kFileHeader{0x534B5653};

/// @brief File header size.
const size_t kFileHeaderSize{sizeof(kFileHeader)};

/// @brief File format version.
const uint32_t kVersion{0};

/// @brief File format version size.
const size_t kVersionSize{sizeof(kVersion)};

// /// @brief Permissions of KVS JSON and hash files: 0640.
// constexpr score::os::Stat::Mode kKvsFileMode{score::os::Stat::Mode::kReadUser | score::os::Stat::Mode::kWriteUser |
//                                              score::os::Stat::Mode::kReadGroup};

/// @brief Check file exists.
/// @param path Path to check.
/// @return Empty result if file of given path exist, an `ErrorCode` otherwise.
/// @pre `filesystem` is not null.
score::Result<void> CheckFileExists(score::filesystem::Filesystem* filesystem,
                                    const score::filesystem::Path& path) noexcept
{
    SCORE_LANGUAGE_FUTURECPP_PRECONDITION_PRD_MESSAGE(filesystem != nullptr, "filesystem must not be null");

    auto result{filesystem->standard->Exists(path)};
    // If unable to determine - physical storage failure.
    if (!result.has_value())
    {
        return score::MakeUnexpected(ErrorCode::kPhysicalStorageFailure);
    }

    // Report file not found.
    if (!*result)
    {
        return score::MakeUnexpected(ErrorCode::kFileNotFound);
    }

    return {};
}
}  // namespace

score::Result<std::string> score::kvs::LoadFile(score::filesystem::Filesystem* filesystem,
                                                const score::filesystem::Path& path) noexcept
{
    SCORE_LANGUAGE_FUTURECPP_PRECONDITION_PRD_MESSAGE(filesystem != nullptr, "filesystem must not be null");

    const score::os::Unistd& unistd{score::os::Unistd::instance()};

    // Check file exists.
    auto file_exists_result{CheckFileExists(filesystem, path)};
    if (!file_exists_result.has_value())
    {
        return score::MakeUnexpected(*file_exists_result.error());
    }

    // Open a file.
    using Open = score::os::Fcntl::Open;
    const Open kFlags{Open::kReadOnly};
    auto open_result{score::os::Fcntl::instance().open(path.CStr(), kFlags)};
    if (!open_result.has_value())
    {
        // TODO: logging.
        return score::MakeUnexpected(ErrorCode::kPhysicalStorageFailure);
    }
    const int32_t fd{open_result.value()};

    // Close file on every early return.
    // Close errors are ignored here because another error is already being reported.
    score::utils::ScopeExit fd_guard{[&unistd, fd]() {
        score::cpp::ignore = unistd.close(fd);
    }};

    // Get file size.
    // Must at least contain file header, version, and checksum.
    // Allow empty data, data layout unknown here.
    const size_t kMinimumSize{kFileHeaderSize + kVersionSize + kHashSize};
    size_t file_size{0};
    {
        const score::os::Stat& stat{score::os::Stat::instance()};
        score::os::StatBuffer stat_buffer{};
        auto fstat_result{stat.fstat(fd, stat_buffer)};
        if (!fstat_result.has_value())
        {
            return score::MakeUnexpected(ErrorCode::kPhysicalStorageFailure);
        }
        file_size = stat_buffer.st_size;
        if (file_size < kMinimumSize)
        {
            return score::MakeUnexpected(ErrorCode::kKvsFileReadError);
        }
    }

    // Read file header, version, and checksum.
    uint32_t file_header{0};
    uint32_t version{0};
    uint32_t checksum{0};
    {
        std::array<uint32_t, 3> file_preamble;
        auto read_result{unistd.read(fd, &file_preamble, sizeof(file_preamble))};
        if (!read_result.has_value())
        {
            // TODO: logging.
            return score::MakeUnexpected(ErrorCode::kKvsFileReadError);
        }

        file_header = file_preamble[0];
        version = file_preamble[1];
        checksum = file_preamble[2];
    }

    // Check file header.
    if (file_header != kFileHeader)
    {
        // It's not known here if file is invalid or corrupted.
        return score::MakeUnexpected(ErrorCode::kKvsFileReadError);
    }

    // Check file format version.
    if (version != kVersion)
    {
        // TODO: new class of error here?
        return score::MakeUnexpected(ErrorCode::kUnmappedError);
    }

    // Read data until EOF.
    // Reserve the expected size up front, but keep reading in case the file grew after `fstat`.
    std::string data{};
    data.reserve(file_size - kMinimumSize);
    {
        const size_t kBufferSize{4096};
        std::array<char, kBufferSize> buffer{};
        while (true)
        {
            auto read_result{unistd.read(fd, buffer.data(), buffer.size())};
            if (!read_result.has_value())
            {
                // Retry if interrupted by a signal before any data was read.
                if (read_result.error() == score::os::Error::Code::kOperationWasInterruptedBySignal)
                {
                    continue;
                }
                return score::MakeUnexpected(ErrorCode::kKvsFileReadError);
            }

            // Zero bytes read means EOF.
            const ssize_t bytes_read{read_result.value()};
            if (bytes_read == 0)
            {
                break;
            }

            data.append(buffer.data(), static_cast<size_t>(bytes_read));
        }
    }

    // Compare checksum.
    auto calculated_checksum{CalculateHashAdler32(data)};
    if (calculated_checksum != checksum)
    {
        return score::MakeUnexpected(ErrorCode::kIntegrityCorrupted);
    }

    // Close file.
    fd_guard.Release();
    auto close_result{unistd.close(fd)};
    if (!close_result.has_value())
    {
        // TODO: logging.
        return score::MakeUnexpected(ErrorCode::kPhysicalStorageFailure);
    }

    return score::MakeUnexpected(ErrorCode::kUnmappedError);
}

score::Result<void> score::kvs::SaveFile(const score::filesystem::Path& path, const std::string_view data) noexcept
{
    return score::MakeUnexpected(ErrorCode::kUnmappedError);
}
