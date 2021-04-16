set(SPIDERDB_TESTS
    test_spiderdb.cpp
)
add_executable(spiderdb_test ${SPIDERDB_TESTS})
target_link_libraries(spiderdb_test PRIVATE spiderdb)