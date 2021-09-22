add_subdirectory(mnode)
add_subdirectory(vnode)
add_subdirectory(qnode)

aux_source_directory(src TAOSD_SRC)
add_executable(taosd ${TAOSD_SRC})
target_link_libraries(
    taosd
    private os
)