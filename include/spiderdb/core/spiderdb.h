//
// Created by chungphb on 16/4/21.
//

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>

namespace spiderdb {

seastar::future<> run();

}