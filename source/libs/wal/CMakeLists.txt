aux_source_directory(src WAL_SRC)
add_library(wal STATIC ${WAL_SRC})
target_include_directories(
    wal
    PUBLIC "${CMAKE_SOURCE_DIR}/include/libs/wal"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
    wal
    PUBLIC cjson
    PUBLIC os
    PUBLIC util
    PUBLIC common
)

if(${BUILD_TEST})
    IF (NOT TD_DARWIN)
        add_subdirectory(test)
    ENDIF ()
endif(${BUILD_TEST})
