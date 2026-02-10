#!/usr/bin/env python3
"""
Functional Identity Test for Persistency Module

This test verifies that the Rust and C++ implementations of the Persistency module
produce functionally identical behavior by running their respective demo applications
and comparing their outputs.
"""

import subprocess
import tempfile
import os
import sys
import shutil

def run_command(cmd, cwd=None, env=None):
    """Run a command and return (returncode, stdout, stderr)"""
    result = subprocess.run(cmd, shell=True, cwd=cwd, env=env,
                          capture_output=True, text=True)
    return result.returncode, result.stdout, result.stderr

def run_demo(binary_path, temp_dir):
    """Run a demo binary and capture its output"""
    env = os.environ.copy()
    env['TMPDIR'] = temp_dir
    returncode, stdout, stderr = run_command(binary_path, env=env)
    return returncode, stdout, stderr

def main():
    print("=== Functional Identity Test ===")

    # Create temporary directory for test
    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"Using temporary directory: {temp_dir}")

        # Build the demo binaries
        print("Building demo binaries...")
        build_cmd = "bazel build //src/cpp/src:demo //src/rust/rust_kvs:demo"
        returncode, stdout, stderr = run_command(build_cmd)
        if returncode != 0:
            print(f"Build failed:\n{stderr}")
            return 1
        print("Build successful")

        # Get paths to built binaries
        cpp_demo = "bazel-bin/src/cpp/src/demo"
        rust_demo = "bazel-bin/src/rust/rust_kvs/demo"

        # Run C++ demo
        print("Running C++ demo...")
        cpp_returncode, cpp_stdout, cpp_stderr = run_demo(cpp_demo, temp_dir)
        if cpp_returncode != 0:
            print(f"C++ demo failed:\n{cpp_stderr}")
            return 1
        print("C++ demo completed")

        # Run Rust demo
        print("Running Rust demo...")
        rust_returncode, rust_stdout, rust_stderr = run_demo(rust_demo, temp_dir)
        if rust_returncode != 0:
            print(f"Rust demo failed:\n{rust_stderr}")
            return 1
        print("Rust demo completed")

        # Compare outputs
        print("Comparing outputs...")

        # Normalize outputs (remove timestamps, paths, etc.)
        def normalize_output(output):
            lines = []
            for line in output.split('\n'):
                # Remove lines with timestamps or file paths
                if '===' in line or line.startswith('1.') or line.startswith('2.') or \
                   line.startswith('3.') or line.startswith('4.') or line.startswith('5.') or \
                   line.startswith('6.') or 'Demo completed' in line:
                    lines.append(line.strip())
                elif 'Stored:' in line or 'Read:' in line or 'Overwritten:' in line:
                    # Extract key-value pairs
                    parts = line.split('=')
                    if len(parts) == 2:
                        lines.append(f"{parts[0].strip()} = {parts[1].strip()}")
            return '\n'.join(lines)

        cpp_normalized = normalize_output(cpp_stdout)
        rust_normalized = normalize_output(rust_stdout)

        print("C++ Output:")
        print(cpp_normalized)
        print("\nRust Output:")
        print(rust_normalized)

        if cpp_normalized == rust_normalized:
            print("\n✅ Functional identity verified: C++ and Rust implementations produce identical behavior")
            return 0
        else:
            print("\n❌ Functional identity failed: Outputs differ")
            return 1

if __name__ == "__main__":
    sys.exit(main())
