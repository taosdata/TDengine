aux_source_directory(src RREADER_SRC)

add_executable(rocks-reader ${RREADER_SRC} ./rreader.cpp)

DEP_td_rocksdb(rocks-reader)

target_link_libraries(rocks-reader
    PUBLIC pthread
    PUBLIC m
)

target_include_directories(
    rocks-reader
    PRIVATE "${CMAKE_BINARY_DIR}/build/include"
)
