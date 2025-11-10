#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "tracing.hpp"
#include "cli.hpp"
#include "scenario.hpp"
#include "test_basic.hpp"
#include "test_context.hpp"
#include "cit/test_default_values.hpp"


__attribute__((constructor))
static void disable_buffering() {
    setvbuf(stdout, NULL, _IONBF, 0);
    setvbuf(stderr, NULL, _IONBF, 0);
}

extern "C" int process_value(int x) {
    std::cout << "[C++] process_value(" << x << ")" << std::endl;
    std::cerr << "[C++] debug stderr " << x << std::endl;
    return x * 2 + 1;
}


void print_scenarios(const ScenarioGroup::Ptr& group, const std::string& prefix = "") {
    std::string group_name = group->name();
    std::string new_prefix = prefix.empty() ? group_name : prefix + "." + group_name;
    for (const auto& scenario : group->scenarios()) {
        TRACING_INFO ("Available scenario: ",std::pair{std::string{"example_key"}, scenario->name()} );
    }
    for (const auto& subgroup : group->groups()) {
        print_scenarios(subgroup, new_prefix);
    }
}

int main(int argc, char** argv) {
   TRACING_INFO("[DEBUG] Entered main()", std::pair{std::string{"argc"}, argc});
   // Print all received arguments for debug
   std::cout << "[DEBUG] argv dump:" << std::endl;
   for (int i = 0; i < argc; ++i) {
       std::cout << "  argv[" << i << "]: " << argv[i] << std::endl;
   }
    try {
        // If called with 3 arguments, treat as direct scenario invocation (for default_values)
        if (argc == 3) {
            std::string scenario_name = argv[1];
            std::string input_json = argv[2];
            // Use C++ default value scenarios
            auto scenarios = get_default_value_scenarios();
            for (const auto& scenario : scenarios) {
                if (scenario->name() == scenario_name) {
                    scenario->run(input_json); // run is void
                    return 0;
                }
            }
            std::cerr << "Scenario not found: " << scenario_name << std::endl;
            return 1;
        }

        // Otherwise, use the CLI/group-based approach (for basic/grouped scenarios)
        std::vector<std::string> raw_arguments{argv, argv + argc};

        // Basic group.
        TRACING_INFO("[DEBUG] Constructing BasicScenario", std::pair{std::string{"example_key"}, "BasicScenario"});
        Scenario::Ptr basic_scenario{new BasicScenario{}};
        ScenarioGroup::Ptr basic_group{new ScenarioGroupImpl{"basic", {basic_scenario}, {}}};

        // Default values group.
        TRACING_INFO("[DEBUG] Constructing DefaultValuesScenario", std::pair{std::string{"example_key"}, "DefaultValuesScenario"});
        Scenario::Ptr default_values_scenario{new DefaultValuesScenario{}};
        TRACING_INFO("[DEBUG] Constructing RemoveKeyScenario", std::pair{std::string{"example_key"}, "RemoveKeyScenario"});
        Scenario::Ptr remove_key_scenario{new RemoveKeyScenario{}};
        TRACING_INFO("[DEBUG] Constructing ResetAllKeysScenario", std::pair{std::string{"example_key"}, "ResetAllKeysScenario"});
        Scenario::Ptr reset_all_keys_scenario{new ResetAllKeysScenario{}};
        TRACING_INFO("[DEBUG] Constructing ResetSingleKeyScenario", std::pair{std::string{"example_key"}, "ResetSingleKeyScenario"});
        Scenario::Ptr reset_single_key_scenario{new ResetSingleKeyScenario{}};
        TRACING_INFO("[DEBUG] Constructing ChecksumScenario", std::pair{std::string{"example_key"}, "ChecksumScenario"});
        Scenario::Ptr checksum_scenario{new ChecksumScenario{}};

        // Group: default_values
        TRACING_INFO("[DEBUG] Constructing default_values_group", std::pair{std::string{"example_key"}, "default_values_group"});
        ScenarioGroup::Ptr default_values_group{new ScenarioGroupImpl{
            "default_values",
            {default_values_scenario, remove_key_scenario, reset_all_keys_scenario, reset_single_key_scenario, checksum_scenario},
            {}
        }};
        // Group: cit
        TRACING_INFO("[DEBUG] Constructing cit_group", std::pair{std::string{"example_key"}, "cit_group"});
        ScenarioGroup::Ptr cit_group{new ScenarioGroupImpl{
            "cit",
            {},
            {default_values_group}
        }};

        // Root group.
        TRACING_INFO("[DEBUG] Constructing root_group", std::pair{std::string{"example_key"}, "root_group"});
        ScenarioGroup::Ptr root_group{new ScenarioGroupImpl{"root", {}, {basic_group, cit_group}}};

        // Run.
        TRACING_INFO("[DEBUG] Constructing TestContext and running CLI", std::pair{std::string{"example_key"}, "TestContext"});
        TestContext test_context{root_group};
        print_scenarios(root_group);
        run_cli_app(raw_arguments, test_context);
    } catch (const std::exception& ex) {
        std::cerr << ex.what() << std::endl;
        return 1;
    }  catch (const std::bad_variant_access& e) {
        std::cerr << "[EXCEPTION] std::bad_variant_access: " << e.what() << std::endl;
        return 99;
    } catch (const std::exception& e) {
        std::cerr << "[EXCEPTION] std::exception: " << e.what() << std::endl;
        return 98;
    } catch (...) {
        std::cerr << "[EXCEPTION] Unknown exception" << std::endl;
        return 97;
    }
    return 0;
}
