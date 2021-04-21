//
// Created by chungphb on 21/4/21.
//

#define SPIDERDB_USING_MASTER_TEST_SUITE
#include <spiderdb/util/string.h>
#include <spiderdb/testing/test_case.h>
#include <cstring>

SPIDERDB_TEST_SUITE(string_test)

SPIDERDB_TEST_CASE(test_create_string) {
    { // Create a string using const char*
        const char* data = "String";
        const size_t len = strlen(data) + 1;
        spiderdb::string str{data, len};
        SPIDERDB_CHECK(str.length() == len);
        SPIDERDB_CHECK(strcmp(str.c_str(), "String") == 0);
    }
    { // Create a string using std::string
        spiderdb::string str{"String"};
        SPIDERDB_CHECK(!str.empty());
        SPIDERDB_CHECK(strcmp(str.c_str(), "String") == 0);
    }
    { // Create a string using seastar::temporary_buffer
        const char* data = "String";
        seastar::temporary_buffer<char> buffer{data, strlen(data)};
        spiderdb::string str{std::move(buffer)};
        SPIDERDB_CHECK(!str.empty());
        SPIDERDB_CHECK(strcmp(str.c_str(), "String") == 0);
    }
    { // Create a string using char arrays
        char arr[] = "String";
        const size_t len = sizeof(arr) / sizeof(*arr);
        spiderdb::string str{arr, len};
        SPIDERDB_CHECK(str.length() == len);
        SPIDERDB_CHECK(strcmp(str.c_str(), "String") == 0);
    }
    { // Create a string using unsigned char arrays
        unsigned char arr[] = {'S', 't', 'r', 'i', 'n', 'g', '\0'};
        const size_t len = sizeof(arr) / sizeof(*arr);
        spiderdb::basic_string<unsigned char> str{arr, len};
        SPIDERDB_CHECK(str.length() == len);
    }
    { // Create a string using signed char arrays
        signed char arr[] = {'S', 't', 'r', 'i', 'n', 'g', '\0'};
        const size_t len = sizeof(arr) / sizeof(*arr);
        spiderdb::basic_string<signed char> str{arr, len};
        SPIDERDB_CHECK(str.length() == len);
    }
    { // Create a string using uint8_t arrays
        uint8_t arr[] = {83, 116, 114, 105, 110, 103, 0};
        const size_t len = sizeof(arr) / sizeof(*arr);
        spiderdb::basic_string<uint8_t> str{arr, len};
        SPIDERDB_CHECK(str.length() == len);
    }
    { // Create a string using int8_t arrays
        int8_t arr[] = {83, 116, 114, 105, 110, 103, 0};
        const size_t len = sizeof(arr) / sizeof(*arr);
        spiderdb::basic_string<int8_t> str{arr, len};
        SPIDERDB_CHECK(str.length() == len);
    }
    { // Failed to create a string using arrays of other types
        // int arr[] = {1000, 2000, 3000, 4000};
        // spiderdb::basic_string<int> str{arr, sizeof(arr) / sizeof(*arr)};
    }
    return seastar::now();
}

SPIDERDB_TEST_CASE(test_empty_string) {
    { // Create an empty string with length 0
        const char* data = nullptr;
        const size_t len = 0;
        spiderdb::string str{data, len};
        SPIDERDB_CHECK(str.empty());
    }
    { // Create an empty string with non-zero length
        const char* data = nullptr;
        const size_t len = 4;
        try {
            spiderdb::string str{data, len};
            SPIDERDB_REQUIRE(false);
        } catch (std::runtime_error& error) {
            SPIDERDB_CHECK(strcmp(error.what(), "Non-zero length empty string") == 0);
        }
    }
    return seastar::now();
}

SPIDERDB_TEST_CASE(test_copy_string) {
    { // Test copy constructor
        spiderdb::string str1{"String"};
        spiderdb::string str2{str1};
        SPIDERDB_CHECK(str1.str() != str2.str());
        SPIDERDB_CHECK(str1 == str2);
    }
    { // Test copy assignment operator
        spiderdb::string str1{"String"};
        spiderdb::string str2 = str1;
        SPIDERDB_CHECK(str1.str() != str2.str());
        SPIDERDB_CHECK(str1 == str2);
    }
    { // Test copy an empty string
        const char* data = nullptr;
        const size_t len = 0;
        spiderdb::string str1{data, len};
        spiderdb::string str2 = str1;
        SPIDERDB_CHECK(str1.empty() && str2.empty());
    }
    return seastar::now();
}

SPIDERDB_TEST_CASE(test_move_string) {
    { // Test copy constructor
        spiderdb::string str1{"String"};
        spiderdb::string str2{std::move(str1)};
        SPIDERDB_CHECK(strcmp(str2.c_str(), "String") == 0);
        SPIDERDB_CHECK(str1.empty());
    }
    { // Test copy assignment operator
        spiderdb::string str1{"String"};
        spiderdb::string str2 = std::move(str1);
        SPIDERDB_CHECK(strcmp(str2.c_str(), "String") == 0);
        SPIDERDB_CHECK(str1.empty());
    }
    { // Test copy an empty string
        const char* data = nullptr;
        const size_t len = 0;
        spiderdb::string str1{data, len};
        spiderdb::string str2 = std::move(str1);
        SPIDERDB_CHECK(str1.empty() && str2.empty());
    }
    return seastar::now();
}

SPIDERDB_TEST_SUITE_END()

