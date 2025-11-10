#pragma once
#include "kvs_parameters.hpp"
#include <kvs.hpp>
#include <kvsbuilder.hpp>


static score::mw::per::kvs::Kvs kvs_instance(const KvsParameters& params) {
    using namespace score::mw::per::kvs;
    InstanceId instance_id{params.instance_id};
    KvsBuilder builder{instance_id};
    if (params.need_defaults.has_value()) {
        builder = builder.need_defaults_flag(*params.need_defaults);
    }
    if (params.need_kvs.has_value()) {
        builder = builder.need_kvs_flag(*params.need_kvs);
    }
    if (params.dir.has_value()) {
            builder = builder.dir(std::string(*params.dir));
    }
    return Kvs{*builder.build()};
}