add_subdirectory(mnode)
add_subdirectory(vnode)
add_subdirectory(qnode)
add_subdirectory(dnode)

aux_source_directory(. TAOSD_SRC)
add_executable(taosd ${TAOSD_SRC})
target_link_libraries(
    taosd
    PUBLIC os
)