add_executable(tmq "")
add_executable(tstream "")
add_executable(demoapi "")

target_sources(tmq
    PRIVATE
    "src/tmq.c"
)

target_sources(tstream
    PRIVATE
    "src/tstream.c"
)

target_sources(demoapi
    PRIVATE
    "src/demoapi.c"
)

target_link_libraries(tmq
    taos_static
)

target_link_libraries(tstream
    taos_static
)

target_link_libraries(demoapi
    taos_static
)

target_include_directories(tmq
    PUBLIC "${TD_SOURCE_DIR}/include/os"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_include_directories(tstream
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_include_directories(demoapi
    PUBLIC "${TD_SOURCE_DIR}/include/client"
    PUBLIC "${TD_SOURCE_DIR}/include/os"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

SET_TARGET_PROPERTIES(tmq PROPERTIES OUTPUT_NAME tmq)
SET_TARGET_PROPERTIES(tstream PROPERTIES OUTPUT_NAME tstream)
SET_TARGET_PROPERTIES(demoapi PROPERTIES OUTPUT_NAME demoapi)
