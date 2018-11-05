EXTERNALPROJECT_ADD(spdlog
    GIT_REPOSITORY "https://github.com/gabime/spdlog"
    GIT_TAG "master"
    BUILD_IN_SOURCE 1
    UPDATE_COMMAND ""
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    INSTALL_COMMAND ""
)

SET(SPDLOG_INCLUDE_DIRS
    ${CMAKE_CURRENT_BINARY_DIR}/spdlog-prefix/src/spdlog/include)
