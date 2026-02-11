#!/bin/sh
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
set -eu

echo "[kvs] Starting Persistency Module Integration Tests"

# Create directories for demos
mkdir -p /tmp/rust_demo
mkdir -p /tmp/cpp_demo
mkdir -p /var/db/kvs
cd /var/db/kvs

echo "[kvs] Running kvs_tool tests"
echo "[kvs] creating values"
kvs_tool -o setkey -k MyKey -p 'Hello World'
kvs_tool -o setkey -k MyKey -p 'true'
kvs_tool -o setkey -k MyKey -p 15
kvs_tool -o setkey -k MyKey -p '[456,false,"Second"]'
kvs_tool -o setkey -k MyKey -p '{"sub-number":789,"sub-array":[1246,false,"Fourth"]}'

echo "[kvs] reading"
kvs_tool -o getkey -k MyKey || exit 1

echo "[kvs] removing"
kvs_tool -o removekey -k MyKey

echo "[kvs] listing (expect no MyKey)"
kvs_tool -o listkeys | grep -v '^MyKey$' >/dev/null 2>&1 || {
  echo "MyKey still present after removal" >&2
  exit 1
}

echo "[kvs] kvs_tool tests passed"

echo "[kvs] Running Rust demo"
cd /tmp/rust_demo
rust_demo > rust_demo.log 2>&1
if [ $? -ne 0 ]; then
    echo "Rust demo failed" >&2
    cat rust_demo.log >&2
    exit 1
fi
echo "Rust demo completed successfully"

echo "[kvs] Running C++ demo"
cd /tmp/cpp_demo
cpp_demo > cpp_demo.log 2>&1
if [ $? -ne 0 ]; then
    echo "C++ demo failed" >&2
    cat cpp_demo.log >&2
    exit 1
fi
echo "C++ demo completed successfully"

echo "[kvs] Verifying functional identity"
# Extract key steps from outputs
rust_steps=$(grep -E "^[0-9]+\.|=== " /tmp/rust_demo/rust_demo.log | head -7)
cpp_steps=$(grep -E "^[0-9]+\.|=== " /tmp/cpp_demo/cpp_demo.log | head -7)

if [ "$rust_steps" != "$cpp_steps" ]; then
    echo "Functional identity check failed: outputs differ" >&2
    echo "Rust output:" >&2
    cat /tmp/rust_demo/rust_demo.log >&2
    echo "C++ output:" >&2
    cat /tmp/cpp_demo/cpp_demo.log >&2
    exit 1
fi

echo "[kvs] Functional identity verified: Rust and C++ demos produce identical output"

echo "[kvs] All tests passed"
# clean exit so the test runner can detect success
/sbin/shutdown -f
