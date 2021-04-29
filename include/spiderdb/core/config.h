//
// Created by chungphb on 27/4/21.
//

#pragma once

namespace spiderdb {

struct file_config {
    uint16_t file_header_size = 1 << 12;
    uint16_t page_header_size = 1 << 7;
    uint32_t page_size = 1 << 14;
};

}