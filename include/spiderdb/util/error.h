//
// Created by chungphb on 9/5/21.
//

#pragma once

#include <stdexcept>

namespace spiderdb {

#define SPIDERDB_APPLY_TO_ERROR_CODE(FUNC) \
    FUNC(closed_error, 0)                  \
    FUNC(invalid_page, 100)                \
    FUNC(invalid_file, 200)                \
    FUNC(file_already_opened, 201)         \
    FUNC(file_already_closed, 202)         \
    FUNC(invalid_node, 300)                \
    FUNC(invalid_page_type, 301)           \
    FUNC(key_exists, 302)                  \
    FUNC(key_not_exists, 303)              \
    FUNC(empty_key, 304)                   \
    FUNC(key_too_long, 305)                \
    FUNC(exceeded_max_key_count, 306)      \
    FUNC(child_not_exists, 350)            \
    FUNC(invalid_btree, 400)

#define SPIDERDB_GENERATE_ERROR_CODE(error, code) error = code,
enum struct error_code : uint16_t {
    SPIDERDB_APPLY_TO_ERROR_CODE(SPIDERDB_GENERATE_ERROR_CODE)
    total = UINT16_MAX
};
#undef SPIDERDB_GENERATE_ERROR_CODE

std::string error_code_to_string(error_code code);

std::string format_error_message(error_code code, const std::string& msg);

struct spiderdb_error : std::runtime_error {
public:
    explicit spiderdb_error(error_code code);
    explicit spiderdb_error(error_code code, const std::string& msg);
    error_code get_error_code() const noexcept;
private:
    error_code _code;
};


struct cache_error : std::runtime_error {
public:
    explicit cache_error(const std::string& msg);
};

}