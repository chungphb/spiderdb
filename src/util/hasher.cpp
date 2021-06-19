//
// Created by chungphb on 6/19/21.
//

#include <spiderdb/util/hasher.h>

namespace spiderdb {

size_t hasher(string&& str) {
    size_t res = 5381;
    size_t len = std::min(str.length(), static_cast<size_t>(1 << 6));
    for (auto i = 0; i < len; ++i) {
        res = res * 33 + static_cast<unsigned char>(str[i]);
    }
    return res;
}

}