load("@rules_cc//cc:cc_binary.bzl", "cc_binary")

cc_binary(
    name = "cpp_test_scenarios",
    srcs = [
        "src/main.cpp",
        "src/test_basic.cpp",
        "src/test_basic.hpp",
        "src/cit/test_default_values.cpp",
        "src/cit/test_default_values.hpp",
        "src/helpers/kvs_parameters.hpp",
        "src/helpers/kvs_instance.hpp",
    ],
    copts = [
        "-g",
    ],
    visibility = ["//tests/python_test_cases:__pkg__"],
    deps = [
        "//src/cpp/src:kvs_cpp",
        "@testing-utils//test_scenarios_cpp:test_scenarios_cpp",
    ],
)
