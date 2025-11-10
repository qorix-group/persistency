#pragma once
#include <optional>
#include <string>
#include <cstdint>

#include <kvs.hpp>
#include <kvsbuilder.hpp>
#include <nlohmann/json.hpp>
#include "score/json/json_parser.h"
#include "score/json/json_writer.h"


namespace {

struct KvsParameters {
    uint64_t instance_id;
    std::optional<bool> need_defaults;
    std::optional<bool> need_kvs;
    std::optional<std::string> dir;
};

KvsParameters map_to_params(const std::string& data) {
    using namespace score::json;

    JsonParser parser;
    auto any_res{parser.FromBuffer(data)};
    if (!any_res) {
        throw std::runtime_error{"Failed to parse JSON data"};
    }
    const auto& map_root{any_res.value().As<Object>().value().get().at("kvs_parameters")};
    const auto& obj_root{map_root.As<Object>().value().get()};

    KvsParameters params;
    params.instance_id = obj_root.at("instance_id").As<double>().value();
    if (obj_root.find("need_defaults") != obj_root.end()) {
        params.need_defaults = obj_root.at("need_defaults").As<bool>().value();
    }
    if (obj_root.find("need_kvs") != obj_root.end()) {
        params.need_kvs = obj_root.at("need_kvs").As<bool>().value();
    }
    if (obj_root.find("dir") != obj_root.end()) {
        params.dir = obj_root.at("dir").As<std::string>().value();
    }

    return params;
}

}  // namespace