load("@rules_python//python:defs.bzl", "py_test")

py_test(
    name = "test_functional_identity",
    srcs = ["test_functional_identity.py"],
    data = [
        "//src/rust/rust_kvs:demo",
        "//src/cpp/src:demo",
    ],
)
