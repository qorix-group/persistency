# *******************************************************************************
# Copyright (c) 2025 Contributors to the Eclipse Foundation
#
# See the NOTICE file(s) distributed with this work for additional
# information regarding copyright ownership.
#
# This program and the accompanying materials are made available under the
# terms of the Apache License Version 2.0 which is available at
# https://www.apache.org/licenses/LICENSE-2.0
#
# SPDX-License-Identifier: Apache-2.0
# *******************************************************************************
load("@rules_cc//cc:cc_binary.bzl", "cc_binary")

cc_binary(
    name = "cpp_test_scenarios",
    srcs = [
        "src/cit/test_default_values.cpp",
        "src/cit/test_default_values.hpp",
        "src/helpers/kvs_instance.hpp",
        "src/helpers/kvs_parameters.hpp",
        "src/main.cpp",
        "src/test_basic.cpp",
        "src/test_basic.hpp",
    ],
    copts = [
        "-g",
        "-Isrc",
        "-Isrc/cit",
        "-Isrc/helpers",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/cpp/src:kvs_cpp",
        "@score_test_scenarios//test_scenarios_cpp",
    ],
)
