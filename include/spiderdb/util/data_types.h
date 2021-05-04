//
// Created by chungphb on 26/4/21.
//

#pragma once

namespace spiderdb {

using page_id = int64_t;
constexpr page_id null_page = -1;

using node_id = int64_t;
constexpr node_id null_node = -1;
constexpr node_id root_node = 0;

using data_pointer = int64_t;
constexpr data_pointer null_data_pointer = -1;

union pointer {
    node_id child;
    data_pointer pointer;
};

using node_item = std::pair<const string&, data_pointer>;

enum struct page_type : uint8_t {
    unused, primary, overflow, internal, leaf
};

using node_type = page_type;

}