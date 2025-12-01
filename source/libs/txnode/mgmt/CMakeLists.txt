aux_source_directory(src TXNODE_MGMT_SRC)
add_library(txnode_mgmt STATIC ${TXNODE_MGMT_SRC})

target_include_directories(
    txnode_mgmt
    PUBLIC
    "${TD_SOURCE_DIR}/include/libs/txnode"
    "${TD_SOURCE_DIR}/include/util"
    "${TD_SOURCE_DIR}/include/common"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
    tmqtt_mgmt
    PRIVATE os util common nodes transport
)
