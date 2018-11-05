# Resources:
#  - http://zeromq.org/intro:get-the-software
#  - http://bit.ly/2dK0UBT
#
# Remember to have libtool, pkg-config, build-essential, autoconf, and automake
# installed.
ExternalProject_Add(zeromqcpp_project
    GIT_REPOSITORY "https://github.com/zeromq/cppzmq.git"
    GIT_TAG "master"
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}
    BUILD_IN_SOURCE 1
    UPDATE_COMMAND ""
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    INSTALL_COMMAND ""
)
SET(ZEROMQCPP_PROJECT zeromqcpp_project)

INCLUDE_DIRECTORIES(SYSTEM ${CMAKE_CURRENT_BINARY_DIR}/src/zeromqcpp_project)