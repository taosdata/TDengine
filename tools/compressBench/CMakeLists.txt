IF(BUILD_TOOLS)
    add_executable(compressBench compressBench.c)

    target_link_libraries(compressBench PRIVATE util m)

    target_include_directories(
        compressBench
        PRIVATE
            ${TD_SOURCE_DIR}/include/util
            ${TD_SOURCE_DIR}/include/common
            ${TD_SOURCE_DIR}/include/os
    )
ENDIF()
