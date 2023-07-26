cmake_minimum_required(VERSION 3.8)

project(deps-download NONE)

include(ExternalProject)

# taosadapter
ExternalProject_Add(taosadapter
        GIT_REPOSITORY https://github.com/taosdata/taosadapter.git
        GIT_TAG main
        SOURCE_DIR "/home/work/TDengine/tools/taosadapter"
        BINARY_DIR ""
        #BUILD_IN_SOURCE TRUE
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ""
        INSTALL_COMMAND ""
        TEST_COMMAND ""
)

# lz4
ExternalProject_Add(lz4
        GIT_REPOSITORY https://github.com/taosdata-contrib/lz4.git
        GIT_TAG v1.9.3
        SOURCE_DIR "/home/work/TDengine/contrib/lz4"
        BINARY_DIR ""
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ""
        INSTALL_COMMAND ""
        TEST_COMMAND ""
        )
# zlib
ExternalProject_Add(zlib
        GIT_REPOSITORY https://github.com/taosdata-contrib/zlib.git
        GIT_TAG v1.2.11
        SOURCE_DIR "/home/work/TDengine/contrib/zlib"
        BINARY_DIR ""
        #BUILD_IN_SOURCE TRUE
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ""
        INSTALL_COMMAND ""
        TEST_COMMAND ""
)
# cjson
ExternalProject_Add(cjson
        GIT_REPOSITORY https://github.com/taosdata-contrib/cJSON.git
        GIT_TAG v1.7.15
        SOURCE_DIR "/home/work/TDengine/contrib/cJson"
        BINARY_DIR ""
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ""
        INSTALL_COMMAND ""
        TEST_COMMAND ""
        )
# libuv
ExternalProject_Add(libuv
        GIT_REPOSITORY https://github.com/libuv/libuv.git
        GIT_TAG v1.44.2
        SOURCE_DIR "/home/work/TDengine/contrib/libuv"
        BINARY_DIR "/home/work/TDengine/contrib/libuv"
        CONFIGURE_COMMAND "" 
        BUILD_COMMAND ""  
        INSTALL_COMMAND ""
        TEST_COMMAND ""
        )

# geos
ExternalProject_Add(geos
        GIT_REPOSITORY https://github.com/libgeos/geos.git
        GIT_TAG 3.12.0
        SOURCE_DIR "/home/work/TDengine/contrib/geos"
        BINARY_DIR ""
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ""
        INSTALL_COMMAND ""
        TEST_COMMAND ""
        )
