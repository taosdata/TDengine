aux_source_directory(src TMQTT_MGMT_SRC)
add_library(tmqtt_mgmt STATIC ${TMQTT_MGMT_SRC})

target_include_directories(
    tmqtt_mgmt
    PUBLIC
    "${TD_SOURCE_DIR}/include/libs/tmqtt"
    "${TD_SOURCE_DIR}/include/util"
    "${TD_SOURCE_DIR}/include/common"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
    tmqtt_mgmt
    PRIVATE os util common nodes transport
)
