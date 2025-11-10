#include <variant>

// Helper to stringify score::Result<KvsValue> for logging

#include "test_default_values.hpp"
#include "../helpers/kvs_parameters.hpp"
#include "../helpers/kvs_instance.hpp"
#include <iostream>
#include <string>
#include <vector>
#include <memory>
#include <optional>
#include "tracing.hpp"
#include <kvs.hpp>
#include <kvsbuilder.hpp>
#include <nlohmann/json.hpp>
#include "score/json/json_parser.h"
#include "score/json/json_writer.h"
#include "score/result/result.h"
#include <type_traits>
#include <sstream>
#include <kvsvalue.hpp>

// Helper to print info logs in a Python-parsable way
const std::string kTargetName{"cpp_test_scenarios::cit::default_values"};
using score::mw::per::kvs::KvsValue;

static std::string result_kvs_value_to_string(const score::Result<KvsValue>& res) {
    if (res.has_value()) {
        const auto& val = res.value();
        switch (val.getType()) {
            case KvsValue::Type::f64: {
                try {
                    std::ostringstream oss;
                    oss.precision(1);
                    oss << std::fixed << std::get<double>(val.getValue());
                    return "Ok(F64(" + oss.str() + "))";
                } catch (...) { return "Err(BadVariantAccess:F64)"; }
            }
            case KvsValue::Type::i64:
                try {
                    return "Ok(I64(" + std::to_string(std::get<int64_t>(val.getValue())) + "))";
                } catch (...) { return "Err(BadVariantAccess:I64)"; }
            case KvsValue::Type::u64:
                try {
                    return "Ok(U64(" + std::to_string(std::get<uint64_t>(val.getValue())) + "))";
                } catch (...) { return "Err(BadVariantAccess:U64)"; }
            case KvsValue::Type::Boolean:
                try {
                    return std::get<bool>(val.getValue()) ? "Ok(Bool(true))" : "Ok(Bool(false))";
                } catch (...) { return "Err(BadVariantAccess:Bool)"; }
            case KvsValue::Type::String:
                try {
                    return "Ok(String(\"" + std::get<std::string>(val.getValue()) + "\"))";
                } catch (...) { return "Err(BadVariantAccess:String)"; }
            default:
                return "Err(UnexpectedType:" + std::to_string(static_cast<int>(val.getType())) + ")";
        }
    } else {
        // Error case: print error code
        auto err = res.error();
        // If error is KeyNotFound, return exactly 'Err(KeyNotFound)'
        // Otherwise, print generic error
        std::ostringstream err_ss;
        err_ss << err;
        std::string err_str = err_ss.str();
        // Try to match string or int value for KeyNotFound
        // If ErrorCode is an enum, you may need to compare to its value
        // For now, use string comparison
        if (err_str == "KeyNotFound" || err_str == "4") { // 4 is a common enum value for KeyNotFound
            return "Err(KeyNotFound)";
        }
        // Fallback: check if error string contains 'Key not found'
        std::ostringstream oss;
        oss << err;
        if (oss.str().find("Key not found") != std::string::npos) {
            return "Err(KeyNotFound)";
        }
        // Otherwise, print generic error
        return "Err(ErrorCode::" + oss.str() + ")";
    }
}
static void info_log(const std::string& key, const std::string& value_is_default,
              const std::string& default_value, const std::string& current_value) {
      TRACING_INFO(
        kTargetName, std::pair{std::string{"key"}, key},
        std::pair{std::string{"value_is_default"}, value_is_default},
        std::pair{std::string{"default_value"}, default_value},
         std::pair{std::string{"current_value"}, current_value}
      );
}

std::string DefaultValuesScenario::name() const { return "default_values"; }
void DefaultValuesScenario::run(const std::optional<std::string>& input) const {
    // TRACING_INFO( "[DEBUG] Entered DefaultValuesScenario::run",std::pair{std::string{"key"}, key} );
    using namespace score::mw::per::kvs;
    std::string key = "test_number";
    TRACING_INFO(
        "[DEBUG] About to call map_to_params",
        std::pair{std::string{"key"}, key},
        std::pair{std::string{"input"}, input ? *input : std::string{"<none>"}}
    );
    auto params = map_to_params(*input);
    std::cout << "[DEBUG] map_to_params done" << std::endl << std::flush;
    std::cout << "[DEBUG] About to call kvs_instance" << std::endl << std::flush;
    auto kvs = kvs_instance(params);
    std::cout << "[DEBUG] kvs_instance done" << std::endl << std::flush;
    {
        // First check: log initial state before any set_value
        auto get_default_result = kvs.get_default_value(key);
        auto get_value_result = kvs.get_value(key);
        std::string value_is_default;
        std::string default_value;
        std::string current_value;
        // Default value
        if (!get_default_result.has_value() || get_default_result.value().getType() != KvsValue::Type::f64 || !std::holds_alternative<double>(get_default_result.value().getValue())) {
            default_value = "Err(KeyNotFound)";
        } else {
            std::ostringstream oss;
            oss.precision(1);
            oss << std::fixed << std::get<double>(get_default_result.value().getValue());
            default_value = "Ok(F64(" + oss.str() + "))";
        }
        // Current value
        if (!get_value_result.has_value() || get_value_result.value().getType() != KvsValue::Type::f64 || !std::holds_alternative<double>(get_value_result.value().getValue())) {
            current_value = "Err(KeyNotFound)";
        } else {
            std::ostringstream oss;
            oss.precision(1);
            oss << std::fixed << std::get<double>(get_value_result.value().getValue());
            current_value = "Ok(F64(" + oss.str() + "))";
        }
        // value_is_default
        if (default_value == "Err(KeyNotFound)" || current_value == "Err(KeyNotFound)") {
            value_is_default = "Err(KeyNotFound)";
        } else if (std::abs(std::get<double>(get_default_result.value().getValue()) - std::get<double>(get_value_result.value().getValue())) < 1e-6) {
            value_is_default = "Ok(true)";
        } else {
            value_is_default = "Ok(false)";
        }
        info_log(key, value_is_default, default_value, current_value);
        auto set_result = kvs.set_value(key, KvsValue{432.1});
        if (!set_result) throw std::runtime_error("Failed to set value");
        kvs.flush();
    }
    {
        // Second check: log after set_value and flush
        // - value_is_default: Ok(true) if value == default, Ok(false) if not, Err(KeyNotFound) if default missing
        auto kvs = kvs_instance(params);
        auto get_default_result = kvs.get_default_value(key);
        auto get_value_result = kvs.get_value(key);
        std::string value_is_default = "Ok(false)";
        std::string default_value;
        std::string current_value;
        bool get_default_ok = get_default_result.has_value();
        bool get_value_ok = get_value_result.has_value();
        const KvsValue* def_val = get_default_ok ? &get_default_result.value() : nullptr;
        const KvsValue* cur_val = get_value_ok ? &get_value_result.value() : nullptr;
        // Debug: print types and variant indices before accessing variant
        if (cur_val) {
            std::cout << "[DEBUG] cur_val type: " << static_cast<int>(cur_val->getType())
                      << ", variant index: " << cur_val->getValue().index() << std::endl;
        } else {
            std::cout << "[DEBUG] cur_val is nullptr" << std::endl;
        }
        if (def_val) {
            std::cout << "[DEBUG] def_val type: " << static_cast<int>(def_val->getType())
                      << ", variant index: " << def_val->getValue().index() << std::endl;
        } else {
            std::cout << "[DEBUG] def_val is nullptr" << std::endl;
        }
        // Defensive: check types before accessing variant
        if (!cur_val || !def_val) {
            // If either value is missing, skip variant access
        } else {
            bool both_f64 = cur_val->getType() == def_val->getType() && cur_val->getType() == KvsValue::Type::f64;
            if (both_f64) {
                try {
                    double v = std::get<double>(cur_val->getValue());
                    double d = std::get<double>(def_val->getValue());
                    if (v == d) value_is_default = "Ok(true)";
                } catch (const std::bad_variant_access& e) {
                    std::cerr << "[EXCEPTION] std::bad_variant_access in DefaultValuesScenario::run (second block): " << e.what() << std::endl;
                    std::cerr << "[EXCEPTION] cur_val type: " << static_cast<int>(cur_val->getType())
                              << ", variant index: " << cur_val->getValue().index() << std::endl;
                    std::cerr << "[EXCEPTION] def_val type: " << static_cast<int>(def_val->getType())
                              << ", variant index: " << def_val->getValue().index() << std::endl;
                    throw;
                }
            }
        }
        // Format default_value for log
        if (get_default_ok && def_val->getType() == KvsValue::Type::f64) {
            try {
                std::ostringstream oss;
                oss.precision(1);
                oss << std::fixed << std::get<double>(def_val->getValue());
                default_value = "Ok(F64(" + oss.str() + "))";
            } catch (const std::bad_variant_access& e) {
                std::cerr << "[EXCEPTION] std::bad_variant_access in DefaultValuesScenario::run (default_value, second block): " << e.what() << std::endl;
                std::cerr << "[EXCEPTION] def_val type: " << static_cast<int>(def_val->getType()) << std::endl;
                throw;
            }
        } else if (get_default_ok) {
            default_value = "Err(UnexpectedType:" + std::to_string(static_cast<int>(def_val->getType())) + ")";
        } else {
            default_value = "Err(KeyNotFound)";
        }
        // Format current_value for log
        if (get_value_ok && cur_val->getType() == KvsValue::Type::f64) {
            try {
                std::ostringstream oss;
                oss.precision(1);
                oss << std::fixed << std::get<double>(cur_val->getValue());
                current_value = "Ok(F64(" + oss.str() + "))";
            } catch (const std::bad_variant_access& e) {
                std::cerr << "[EXCEPTION] std::bad_variant_access in DefaultValuesScenario::run (current_value, second block): " << e.what() << std::endl;
                std::cerr << "[EXCEPTION] cur_val type: " << static_cast<int>(cur_val->getType()) << std::endl;
                throw;
            }
        } else if (get_value_ok) {
            current_value = "Err(UnexpectedType:" + std::to_string(static_cast<int>(cur_val->getType())) + ")";
        } else {
            current_value = "Err(KeyNotFound)";
        }
        info_log(key, value_is_default, default_value, current_value); // Log after set/flush
    }
            
}

std::string RemoveKeyScenario::name() const { return "remove_key"; }
void RemoveKeyScenario::run(const std::optional<std::string>& input) const {
    using namespace score::mw::per::kvs;
    std::string key = "test_number";
    auto params = map_to_params(*input);
    auto kvs = kvs_instance(params);
    auto get_default = kvs.get_default_value(key);
    auto get_value = kvs.get_value(key);
    std::string value_is_default;
    std::string default_value;
    std::string current_value;
    // Default value
    if (!get_default || get_default->getType() != KvsValue::Type::f64 || !std::holds_alternative<double>(get_default->getValue())) {
        default_value = "Err(KeyNotFound)";
    } else {
        std::ostringstream oss;
        oss.precision(1);
        oss << std::fixed << std::get<double>(get_default->getValue());
        default_value = "Ok(F64(" + oss.str() + "))";
    }
    // Current value
    if (!get_value || get_value->getType() != KvsValue::Type::f64 || !std::holds_alternative<double>(get_value->getValue())) {
        current_value = "Err(KeyNotFound)";
    } else {
        std::ostringstream oss;
        oss.precision(1);
        oss << std::fixed << std::get<double>(get_value->getValue());
        current_value = "Ok(F64(" + oss.str() + "))";
    }
    // value_is_default
    if (default_value == "Err(KeyNotFound)" || current_value == "Err(KeyNotFound)") {
        value_is_default = "Err(KeyNotFound)";
    } else if (std::abs(std::get<double>(get_default->getValue()) - std::get<double>(get_value->getValue())) < 1e-6) {
        value_is_default = "Ok(true)";
    } else {
        value_is_default = "Ok(false)";
    }
    info_log(key, value_is_default, default_value, current_value);
    auto set_result = kvs.set_value(key, KvsValue{432.1});
    if (!set_result) throw std::runtime_error("Failed to set value");
    get_value = kvs.get_value(key);
    // Second check: log after set_value
    // - value_is_default: Ok(true) if value == default, Ok(false) if not
    value_is_default = "Ok(false)";
    if (get_value && get_default && get_value->getType() == get_default->getType() && get_value->getType() == KvsValue::Type::f64) {
        double v = std::get<double>(get_value->getValue());
        double d = std::get<double>(get_default->getValue());
        if (v == d) value_is_default = "Ok(true)";
    }
    // Format current_value for log
    if (get_value && get_value->getType() == KvsValue::Type::f64) {
        std::ostringstream oss;
        oss.precision(1);
        oss << std::fixed << std::get<double>(get_value->getValue());
        current_value = "Ok(F64(" + oss.str() + "))";
    } else {
        current_value = "Err(KeyNotFound)";
    }
    info_log(key, value_is_default, default_value, current_value); // Log after set
    auto remove_result = kvs.remove_key(key);
    if (!remove_result) throw std::runtime_error("Failed to remove key");
    get_value = kvs.get_value(key);
    // Third check: log after remove_key
    // - value_is_default: Err(KeyNotFound) if default missing, Ok(true) if value == default, Ok(false) otherwise
    if (!get_default) {
        value_is_default = "Err(KeyNotFound)"; // Defensive: default missing after remove
    } else {
        value_is_default = "Ok(false)";
        if (get_value && get_default && get_value->getType() == get_default->getType() && get_value->getType() == KvsValue::Type::f64) {
            double v = std::get<double>(get_value->getValue());
            double d = std::get<double>(get_default->getValue());
            if (v == d) value_is_default = "Ok(true)";
        }
    }
    // Format current_value for log
    if (get_value && get_value->getType() == KvsValue::Type::f64) {
        std::ostringstream oss;
        oss.precision(1);
        oss << std::fixed << std::get<double>(get_value->getValue());
        current_value = "Ok(F64(" + oss.str() + "))";
    } else {
        current_value = "Err(KeyNotFound)";
    }
    info_log(key, value_is_default, default_value, current_value); // Log after remove
          //  info_log(key, "Ok(false)", "TTest", "Test");
}

std::string ResetAllKeysScenario::name() const { return "reset_all_keys"; }
void ResetAllKeysScenario::run(const std::optional<std::string>& input) const {
    using namespace score::mw::per::kvs;
    int num_values = 5;
    auto params = map_to_params(*input);
    auto kvs = kvs_instance(params);
    std::vector<std::pair<std::string, double>> key_values;
    for (int i = 0; i < num_values; ++i) {
        key_values.emplace_back("test_number_" + std::to_string(i), 123.4 * i);
    }
    for (const auto& [key, value] : key_values) {
        auto get_default = kvs.get_default_value(key);
        auto get_value = kvs.get_value(key);
        std::string value_is_default = "Ok(false)";
        std::string current_value;
        if (get_value && get_default && get_value->getType() == get_default->getType() && get_value->getType() == KvsValue::Type::f64) {
            if (std::holds_alternative<double>(get_value->getValue()) && std::holds_alternative<double>(get_default->getValue())) {
                double v = std::get<double>(get_value->getValue());
                double d = std::get<double>(get_default->getValue());
                if (v == d) value_is_default = "Ok(true)";
            } else {
                value_is_default = "Err(UnexpectedType:f64)";
            }
        } else if (!get_default) {
            value_is_default = "Err(KeyNotFound)";
        }
        if (get_value && get_value->getType() == KvsValue::Type::f64 && std::holds_alternative<double>(get_value->getValue())) {
            std::ostringstream oss;
            oss.precision(1);
            oss << std::fixed << std::get<double>(get_value->getValue());
            current_value = "Ok(F64(" + oss.str() + "))";
        } else if (get_value) {
            current_value = "Err(UnexpectedType:" + std::to_string(static_cast<int>(get_value->getType())) + ")";
        } else {
            current_value = "Err(KeyNotFound)";
        }
        info_log(key, value_is_default, "", current_value);
        auto set_result = kvs.set_value(key, KvsValue{value});
        if (!set_result) throw std::runtime_error("Failed to set value");
        get_value = kvs.get_value(key);
        value_is_default = "Ok(false)";
        if (get_value && get_default && get_value->getType() == get_default->getType() && get_value->getType() == KvsValue::Type::f64) {
            if (std::holds_alternative<double>(get_value->getValue()) && std::holds_alternative<double>(get_default->getValue())) {
                double v = std::get<double>(get_value->getValue());
                double d = std::get<double>(get_default->getValue());
                if (v == d) value_is_default = "Ok(true)";
            }
        }
        if (get_value && get_value->getType() == KvsValue::Type::f64 && std::holds_alternative<double>(get_value->getValue())) {
            std::ostringstream oss;
            oss.precision(1);
            oss << std::fixed << std::get<double>(get_value->getValue());
            current_value = "Ok(F64(" + oss.str() + "))";
        } else {
            current_value = "Err(KeyNotFound)";
        }
        info_log(key, value_is_default, "", current_value);
    }
    kvs.reset();
    for (const auto& [key, _] : key_values) {
        auto get_default = kvs.get_default_value(key);
        auto get_value = kvs.get_value(key);
        std::string value_is_default = "Ok(false)";
        std::string current_value;
        if (get_value && get_default && get_value->getType() == get_default->getType() && get_value->getType() == KvsValue::Type::f64) {
            if (std::holds_alternative<double>(get_value->getValue()) && std::holds_alternative<double>(get_default->getValue())) {
                double v = std::get<double>(get_value->getValue());
                double d = std::get<double>(get_default->getValue());
                if (v == d) value_is_default = "Ok(true)";
            } else {
                value_is_default = "Err(UnexpectedType:f64)";
            }
        } else if (!get_default) {
            value_is_default = "Err(KeyNotFound)";
        }
        if (get_value && get_value->getType() == KvsValue::Type::f64 && std::holds_alternative<double>(get_value->getValue())) {
            std::ostringstream oss;
            oss.precision(1);
            oss << std::fixed << std::get<double>(get_value->getValue());
            current_value = "Ok(F64(" + oss.str() + "))";
        } else if (get_value) {
            current_value = "Err(UnexpectedType:" + std::to_string(static_cast<int>(get_value->getType())) + ")";
        } else {
            current_value = "Err(KeyNotFound)";
        }
        info_log(key, value_is_default, "", current_value);
    }
           // info_log("32", "Ok(false)", "TTest", "Test");
}

std::string ResetSingleKeyScenario::name() const { return "reset_single_key"; }
void ResetSingleKeyScenario::run(const std::optional<std::string>& input) const {
    using namespace score::mw::per::kvs;
    int num_values = 5;
    int reset_index = 2;
    auto params = map_to_params(*input);
    auto kvs = kvs_instance(params);
    std::vector<std::pair<std::string, double>> key_values;
    for (int i = 0; i < num_values; ++i) {
        key_values.emplace_back("test_number_" + std::to_string(i), 123.4 * i);
    }
    for (const auto& [key, value] : key_values) {
        // First check: log initial state before set_value
        // - value_is_default: Ok(true) if value == default, Ok(false) if not
        auto get_default = kvs.get_default_value(key);
        auto get_value = kvs.get_value(key);
        std::string value_is_default = "Ok(false)";
        bool both_f64 = get_value && get_default && get_value->getType() == get_default->getType() && get_value->getType() == KvsValue::Type::f64;
        if (both_f64) {
            double v = std::get<double>(get_value->getValue());
            double d = std::get<double>(get_default->getValue());
            if (v == d) value_is_default = "Ok(true)";
        }
        std::string current_value;
        if (get_value && get_value->getType() == KvsValue::Type::f64) {
            current_value = std::to_string(std::get<double>(get_value->getValue()));
        } else if (get_value) {
            current_value = "Err(UnexpectedType:" + std::to_string(static_cast<int>(get_value->getType())) + ")";
        } else {
            current_value = "Err(KeyNotFound)";
        }
        info_log(key, value_is_default, "", current_value); // Log initial check
        auto set_result = kvs.set_value(key, KvsValue{value});
        if (!set_result) throw std::runtime_error("Failed to set value");
        get_value = kvs.get_value(key);
        // Second check: log after set_value
        value_is_default = "Ok(false)";
        if (get_value && get_default && get_value->getType() == get_default->getType() && get_value->getType() == KvsValue::Type::f64) {
            double v = std::get<double>(get_value->getValue());
            double d = std::get<double>(get_default->getValue());
            if (v == d) value_is_default = "Ok(true)";
        }
        current_value = (get_value && get_value->getType() == KvsValue::Type::f64)
            ? std::to_string(std::get<double>(get_value->getValue()))
            : "Err(KeyNotFound)";
        info_log(key, value_is_default, "", current_value); // Log after set
    }
    auto reset_result = kvs.reset_key(key_values[reset_index].first);
    if (!reset_result) throw std::runtime_error("Failed to reset key");
    for (const auto& [key, _] : key_values) {
        // Third check: log after reset_key (single key)
        // - value_is_default: Ok(true) if value == default, Ok(false) if not
        auto get_default = kvs.get_default_value(key);
        auto get_value = kvs.get_value(key);
        std::string value_is_default = "Ok(false)";
        bool both_f64 = get_value && get_default && get_value->getType() == get_default->getType() && get_value->getType() == KvsValue::Type::f64;
        if (both_f64) {
            double v = std::get<double>(get_value->getValue());
            double d = std::get<double>(get_default->getValue());
            if (v == d) value_is_default = "Ok(true)";
        }
        std::string current_value;
        if (get_value && get_value->getType() == KvsValue::Type::f64) {
            current_value = std::to_string(std::get<double>(get_value->getValue()));
        } else if (get_value) {
            current_value = "Err(UnexpectedType:" + std::to_string(static_cast<int>(get_value->getType())) + ")";
        } else {
            current_value = "Err(KeyNotFound)";
        }
        info_log(key, value_is_default, "", current_value); // Log after reset
    }
         //   info_log("34", "Ok(false)", "TTest", "Test");
}

std::string ChecksumScenario::name() const { return "checksum"; }
void ChecksumScenario::run(const std::optional<std::string>& input) const {
    using namespace score::mw::per::kvs;
    auto params = map_to_params(*input);
    auto kvs = kvs_instance(params);
    kvs.flush();
    auto kvs_path_result = kvs.get_kvs_filename(0);
    auto hash_path_result = kvs.get_hash_filename(0);
    std::string kvs_path = kvs_path_result ? static_cast<std::string>(kvs_path_result.value()) : "<error>";
    std::string hash_path = hash_path_result ? static_cast<std::string>(hash_path_result.value()) : "<error>";
    std::cout << "kvs_path=" << kvs_path << " hash_path=" << hash_path << std::endl;
}

std::vector<std::shared_ptr<const Scenario>> get_default_value_scenarios() {
    std::vector<std::shared_ptr<const Scenario>> scenarios;
    scenarios.emplace_back(std::make_shared<DefaultValuesScenario>());
    scenarios.emplace_back(std::make_shared<RemoveKeyScenario>());
    scenarios.emplace_back(std::make_shared<ResetAllKeysScenario>());
    scenarios.emplace_back(std::make_shared<ResetSingleKeyScenario>());
    scenarios.emplace_back(std::make_shared<ChecksumScenario>());
    return scenarios;
}
