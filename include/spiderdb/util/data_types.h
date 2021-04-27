//
// Created by chungphb on 26/4/21.
//

#pragma once

namespace spiderdb {

using page_id = int64_t;
constexpr page_id null_page = -1;

enum struct page_type : uint8_t {
    unused, primary, overflow
};

}