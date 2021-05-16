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

using value_id = int16_t;

enum struct page_type : uint8_t {
    unused = 0,
    overflow = 1,
    internal = 2,
    leaf = 3,
    data = 4
};

using node_type = page_type;

}