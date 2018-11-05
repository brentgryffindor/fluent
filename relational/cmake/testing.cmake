CMAKE_MINIMUM_REQUIRED(VERSION 3.6)

# CREATE_NAMED_TEST(foo bar.cpp) creates a test named `foo` from the file
# `bar.cc`. Refer to [1] and [2] for documentation on how ADD_TEST works.
#
# [1]: http://stackoverflow.com/a/21413672/3187068
# [2]: https://cmake.org/cmake/help/v3.0/command/add_test.html
MACRO(CREATE_NAMED_TEST NAME FILENAME)
    ADD_EXECUTABLE(${NAME} ${FILENAME})
    ADD_TEST(NAME ${NAME} COMMAND ${NAME})
    SET_TESTS_PROPERTIES(${NAME} PROPERTIES LABELS "UNITTEST")
    TARGET_LINK_LIBRARIES(${NAME}
        glog
        gmock
        gmock_main
        gtest
        gtest_main
        pthread)
    ADD_DEPENDENCIES(${NAME}
        ${GOOGLELOG_PROJECT}
        ${GOOGLETEST_PROJECT})
ENDMACRO(CREATE_NAMED_TEST)

# CREATE_TEST(foo) creates a test named `foo` from the file `foo.cpp`.
MACRO(CREATE_TEST NAME)
    CREATE_NAMED_TEST(${NAME} ${NAME}.cpp)
ENDMACRO(CREATE_TEST)