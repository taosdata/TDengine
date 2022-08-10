add_executable(simulate_vnode "simulate_vnode.c")
target_link_libraries(simulate_vnode PUBLIC craft lz4 uv_a)
if(${BUILD_WINGETOPT})
    target_link_libraries(simulate_vnode PUBLIC wingetopt)
    target_include_directories(
        simulate_vnode
        PUBLIC "${TD_SOURCE_DIR}/contrib/wingetopt/src"
    )
endif()