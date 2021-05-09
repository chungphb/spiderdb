//
// Created by chungphb on 9/5/21.
//

#pragma once

#include <stdexcept>

namespace spiderdb {

#define SPIDERDB_APPLY_TO_ERROR_CODE(FUNC) \
    FUNC(first_error, 0)

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

}