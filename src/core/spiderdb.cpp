//
// Created by chungphb on 16/4/21.
//

#include <spiderdb/core/spiderdb.h>

namespace spiderdb {

seastar::future<> run() {
    std::cout << "SpiderDB" << std::endl;
    return seastar::now();
}

}