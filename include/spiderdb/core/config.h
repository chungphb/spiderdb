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
    seastar::log_level log_level = seastar::log_level::info;
};

struct btree_config {
    uint32_t max_keys_on_each_node = 1 << 12;
    uint32_t min_keys_on_each_node = 1 << 4;
    uint32_t n_cached_nodes = 1 << 8;
    bool enable_logging_node_detail = false;
};

struct storage_config {
    uint64_t max_empty_values_on_each_page = 1 << 8;
    uint64_t max_available_pages = 1 << 8;
    uint32_t min_available_space = 1 << 7;
    uint32_t n_cached_data_pages = 1 << 8;
    bool enable_logging_data_page_detail = false;
};

struct spiderdb_config : file_config, btree_config, storage_config {};

}
