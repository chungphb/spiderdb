//
// Created by chungphb on 9/5/21.
//

#include <spiderdb/util/error.h>
#include <unordered_map>
#include <algorithm>

namespace spiderdb {

#define SPIDERDB_GENERATE_ERROR_CODE_STRING(error, code) {static_cast<error_code>(code), #error},
std::unordered_map<error_code, std::string> error_codes = {
    SPIDERDB_APPLY_TO_ERROR_CODE(SPIDERDB_GENERATE_ERROR_CODE_STRING)
    {error_code::total, ""}
};
#undef SPIDERDB_GENERATE_ERROR_CODE_STRING

std::string error_code_to_string(error_code code) {
    return error_codes.find(code) != error_codes.end() ? error_codes[code] : "";
}

std::string format_error_message(error_code code, const std::string& msg) {
    return error_code_to_string(code) + (!msg.empty() ? " (" + msg + ") " : "");
}

spiderdb_error::spiderdb_error(error_code code)
    : spiderdb_error{code, ""} {}

spiderdb_error::spiderdb_error(error_code code, const std::string& msg)
    : std::runtime_error(format_error_message(code, msg)), _code{code} {}

error_code spiderdb_error::get_error_code() const noexcept {
    return _code;
}

}