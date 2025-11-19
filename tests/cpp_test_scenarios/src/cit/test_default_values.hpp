#pragma once

#include "scenario.hpp"
#include <memory>
#include <optional>
#include <string>
#include <vector>

// Each scenario is a class derived from Scenario, similar to Rust

class DefaultValuesScenario final : public Scenario {
public:
  ~DefaultValuesScenario() final = default;
  std::string name() const final;
  void run(const std::optional<std::string> &input) const final;
};

class RemoveKeyScenario final : public Scenario {
public:
  ~RemoveKeyScenario() final = default;
  std::string name() const final;
  void run(const std::optional<std::string> &input) const final;
};

class ResetAllKeysScenario final : public Scenario {
public:
  ~ResetAllKeysScenario() final = default;
  std::string name() const final;
  void run(const std::optional<std::string> &input) const final;
};

class ResetSingleKeyScenario final : public Scenario {
public:
  ~ResetSingleKeyScenario() final = default;
  std::string name() const final;
  void run(const std::optional<std::string> &input) const final;
};

class ChecksumScenario final : public Scenario {
public:
  ~ChecksumScenario() final = default;
  std::string name() const final;
  void run(const std::optional<std::string> &input) const final;
};

// Helper to get all scenarios
std::vector<std::shared_ptr<const Scenario>> get_default_value_scenarios();
