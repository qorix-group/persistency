#!/usr/bin/env python3
"""
Functional Identity Test for Persistency Module

This test verifies that the Rust and C++ implementations of the Persistency module
are functionally identical by running their respective demo applications and
comparing their outputs.
"""

import subprocess
import sys
import tempfile
import os
import re
from pathlib import Path

def run_command(cmd, cwd=None):
    """Run a command and return (returncode, stdout, stderr)"""
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd=cwd)
    return result.returncode, result.stdout, result.stderr

def extract_demo_output(output):
    """Extract the key steps from demo output for comparison"""
    lines = output.strip().split('\n')
    steps = []
    for line in lines:
        if line.startswith('=== ') or line.startswith('1. ') or line.startswith('2. ') or \
           line.startswith('3. ') or line.startswith('4. ') or line.startswith('5. ') or \
           line.startswith('6. '):
            steps.append(line.strip())
    return steps

def main():
    print("=== Functional Identity Test ===")

    # Create temporary directory for test
    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"Using temporary directory: {temp_dir}")

        # Build Rust demo
        print("Building Rust demo...")
        ret, stdout, stderr = run_command("bazel build //src/rust/rust_kvs:demo")
        if ret != 0:
            print(f"Failed to build Rust demo: {stderr}")
            return 1
        print("Rust demo built successfully")

        # Build C++ demo
        print("Building C++ demo...")
        ret, stdout, stderr = run_command("bazel build //src/cpp/src:demo")
        if ret != 0:
            print(f"Failed to build C++ demo: {stderr}")
            return 1
        print("C++ demo built successfully")

        # Run Rust demo
        print("Running Rust demo...")
        ret, rust_output, stderr = run_command("bazel run //src/rust/rust_kvs:demo", cwd=temp_dir)
        if ret != 0:
            print(f"Failed to run Rust demo: {stderr}")
            return 1
        print("Rust demo completed")

        # Run C++ demo
        print("Running C++ demo...")
        ret, cpp_output, stderr = run_command("bazel run //src/cpp/src:demo", cwd=temp_dir)
        if ret != 0:
            print(f"Failed to run C++ demo: {stderr}")
            return 1
        print("C++ demo completed")

        # Extract and compare outputs
        rust_steps = extract_demo_output(rust_output)
        cpp_steps = extract_demo_output(cpp_output)

        print("\n=== Output Comparison ===")
        print("Rust steps:")
        for step in rust_steps:
            print(f"  {step}")
        print("\nC++ steps:")
        for step in cpp_steps:
            print(f"  {step}")

        if rust_steps == cpp_steps:
            print("\n✅ Functional identity verified: Rust and C++ demos produce identical output")
            return 0
        else:
            print("\n❌ Functional identity failed: Outputs differ")
            print("Full Rust output:")
            print(rust_output)
            print("Full C++ output:")
            print(cpp_output)
            return 1

if __name__ == "__main__":
    sys.exit(main())
