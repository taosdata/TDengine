add_executable(simulate_vnode "simulate_vnode.c")
add_dependencies(simulate_vnode ext_libuv)
target_include_directories(simulate_vnode PRIVATE "${TD_CONTRIB_EXT_LIBUV_INC_DIR}")
target_link_directories(simulate_vnode PRIVATE "${TD_CONTRIB_EXT_LIBUV_LIB_DIR}")
target_link_libraries(simulate_vnode PUBLIC craft lz4 uv.a)
if(${BUILD_WINGETOPT})
    target_link_libraries(simulate_vnode PUBLIC wingetopt)
    target_include_directories(
        simulate_vnode
        PUBLIC "${TD_SOURCE_DIR}/contrib/wingetopt/src"
    )
endif()
