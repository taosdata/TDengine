aux_source_directory(src TXNODE_SRC)
add_library(txnode STATIC ${TXNODE_SRC})

target_include_directories(
    txnode
    PUBLIC
    "${TD_SOURCE_DIR}/include/util"
    "${TD_SOURCE_DIR}/include/common"
    "${TD_SOURCE_DIR}/include/libs/txnode"
    PRIVATE 
    "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
    txnode
    PRIVATE os util common nodes transport
)

find_path(HAVE_SYS_EPOLL_H sys/epoll.h)
if (HAVE_SYS_EPOLL_H)
	add_definitions("-DWITH_EPOLL")
endif()

# test
if(${BUILD_TEST})
    #add_subdirectory(test)
endif()

