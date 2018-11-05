EXTERNALPROJECT_ADD(zeromqcpp
    GIT_REPOSITORY "https://github.com/zeromq/cppzmq.git"
    GIT_TAG "master"
    BUILD_IN_SOURCE 1
    UPDATE_COMMAND ""
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    INSTALL_COMMAND ""
)

SET(ZEROMQCPP_INCLUDE_DIRS
    ${CMAKE_CURRENT_BINARY_DIR}/zeromqcpp-prefix/src/zeromqcpp)