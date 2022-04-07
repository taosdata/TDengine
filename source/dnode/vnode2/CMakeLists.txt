# vnode
add_library(vnode2 "")
target_sources(
    vnode2
    PRIVATE
    # vnode
    "src/vnode/vnodeMain.c"
)

target_include_directories(
    vnode2
    PUBLIC "inc"
    PRIVATE "src/inc"
)

target_link_libraries(
    vnode2
    os
)

