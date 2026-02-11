# Persistency Module Automated Integration Test Suite Implementation

## 1. Create Demo Applications
- [x] Create Rust demo: `src/rust/rust_kvs/examples/demo.rs`
- [x] Create C++ demo: `src/cpp/src/demo.cpp`
- [x] Update `src/cpp/src/BUILD` to add demo target
- [x] Update `src/rust/rust_kvs/BUILD` to add demo target

## 2. Create Functional Identity Test
- [x] Create `tests/test_cases/tests/test_functional_identity.py`

## 3. Update QNX Integration Tests
- [x] Update `tests/integration_test_scenarios/BUILD` to stage demo binaries
- [x] Update `tests/integration_test_scenarios/init_rpi4.build` to include demos
- [x] Update `tests/integration_test_scenarios/kvs_test.sh` to run demos and verify

## 4. Add PR Automation
- [x] Create `.github/workflows/ci.yml` for PR automation

## 5. Verify Requirements
- [ ] Ensure no C-bindings in Rust (native API)
- [ ] Confirm OS agnostic (Linux/QNX)
- [ ] Support x86_64 and aarch64 builds

## 6. Testing and Validation
- [x] Build and test demos locally
- [ ] Run integration tests on QEMU
- [ ] Verify x86_64 and aarch64 builds
- [ ] Test PR workflow
