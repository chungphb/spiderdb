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
    internal = 1,
    leaf = 2,
    data = 3,
    overflow = 4
};

using node_type = page_type;

inline const char* page_type_to_string(page_type type) {
    switch (type) {
        case page_type::internal: {
            return "internal";
        }
        case page_type::leaf: {
            return "leaf";
        }
        case page_type::data: {
            return "data";
        }
        case page_type::overflow: {
            return "overflow";
        }
        default: {
            return "unused";
        }
    }
}


}