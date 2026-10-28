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
#ifndef SCORE_KVS_INTERNAL_FILE_HELPER_H
#define SCORE_KVS_INTERNAL_FILE_HELPER_H

#include "score/filesystem/filesystem.h"
#include "score/filesystem/path.h"
#include "score/result/result.h"
#include <string_view>

namespace score::kvs
{
score::Result<std::string> LoadFile(score::filesystem::Filesystem* filesystem,
                                    const score::filesystem::Path& path) noexcept;
score::Result<void> SaveFile(const score::filesystem::Path& path, const std::string_view data) noexcept;
}  // namespace score::kvs

#endif  // SCORE_KVS_INTERNAL_FILE_HELPER_H
