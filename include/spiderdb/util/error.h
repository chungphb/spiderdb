//
// Created by chungphb on 9/5/21.
//

#pragma once

#include <stdexcept>

namespace spiderdb {

#define SPIDERDB_APPLY_TO_ERROR_CODE(FUNC) \
    FUNC(closed_error, 0)                  \
    FUNC(page_unavailable, 100)            \
    FUNC(page_type_incorrect, 101)         \
    FUNC(file_already_opened, 200)         \
    FUNC(node_unavailable, 300)            \
    FUNC(node_exceeded_max_key_count, 301) \
    FUNC(node_child_not_exists, 302)       \
    FUNC(key_exists, 350)                  \
    FUNC(key_not_exists, 351)              \
    FUNC(key_too_short, 352)               \
    FUNC(key_too_long, 353)                \
    FUNC(data_page_unavailable, 400)       \
    FUNC(value_not_exists, 450)            \
    FUNC(value_too_short, 451)

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