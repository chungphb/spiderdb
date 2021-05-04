//
// Created by chungphb on 27/4/21.
//

#pragma once

#include <seastar/util/log.hh>

namespace spiderdb {

struct file_config {
    uint16_t file_header_size = 1 << 12;
    uint16_t page_header_size = 1 << 7;
    uint32_t page_size = 1 << 14;
    seastar::log_level log_level = seastar::log_level::trace;
};

struct btree_config {
    uint32_t max_keys_on_each_node = 1 << 12;
    uint32_t min_keys_on_each_node = 1 << 4;
    uint32_t n_cached_nodes = 1 << 8;
};

struct spiderdb_config : file_config, btree_config {};

}
