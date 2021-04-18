//
// Created by chungphb on 16/4/21.
//

#include <spiderdb/core/spiderdb.h>
#include <spiderdb/util/string.h>
#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>

int main(int argc, char** argv) {
    seastar::app_template app;
    app.run(argc, argv, [] {
        spiderdb::string str1("Hello!");
        spiderdb::string str2("World!");
        if (str1 != str2) {
            std::cout << "Passed" << std::endl;
        } else {
            std::cout << "Failed" << std::endl;
        }
        return spiderdb::run();
    });
    return 0;
}