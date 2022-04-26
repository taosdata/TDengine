add_executable(simulate_vnode "simulate_vnode.c")
target_link_libraries(simulate_vnode PUBLIC craft lz4 uv_a)