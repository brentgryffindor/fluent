SET(CXX "clang++ -stdlib=c++")

if(APPLE)
  set(PREFIX /usr/local)
elseif(UNIX)
  set(PREFIX /usr)
endif()

EXTERNALPROJECT_ADD(zeromq
    URL "https://github.com/zeromq/libzmq/releases/download/v4.2.5/zeromq-4.2.5.tar.gz"
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}
    BUILD_IN_SOURCE 1
    UPDATE_COMMAND ""
    CONFIGURE_COMMAND ./configure
    BUILD_COMMAND make
    INSTALL_COMMAND ""
)

SET(ZEROMQ_INCLUDE_DIRS ${CMAKE_CURRENT_BINARY_DIR}/src/zeromq/include)
SET(ZEROMQ_LINK_DIRS ${CMAKE_CURRENT_BINARY_DIR}/src/zeromq/src/.libs)