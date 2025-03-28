aux_source_directory(src TAOSM_SRC)

add_executable(mqttd ${TAOSM_SRC})

IF(TD_LINUX_64 AND JEMALLOC_ENABLED)
    ADD_DEFINITIONS(-DTD_JEMALLOC_ENABLED -I${CMAKE_BINARY_DIR}/build/include -L${CMAKE_BINARY_DIR}/build/lib -Wl,-rpath,${CMAKE_BINARY_DIR}/build/lib -ljemalloc)
    SET(LINK_JEMALLOC "-L${CMAKE_BINARY_DIR}/build/lib -ljemalloc")
    ADD_DEPENDENCIES(shell jemalloc)
ELSE()
    SET(LINK_JEMALLOC "")
ENDIF()

target_link_libraries(
    mqttd
    PRIVATE taos pthread
)

target_include_directories(
    mqttd
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

SET_TARGET_PROPERTIES(mqttd PROPERTIES OUTPUT_NAME mqttd)

# topic tool
aux_source_directory(tools TOPICTOOL_SRC)

add_executable(topic ${TOPICTOOL_SRC})

IF(TD_LINUX_64 AND JEMALLOC_ENABLED)
    ADD_DEFINITIONS(-DTD_JEMALLOC_ENABLED -I${CMAKE_BINARY_DIR}/build/include -L${CMAKE_BINARY_DIR}/build/lib -Wl,-rpath,${CMAKE_BINARY_DIR}/build/lib -ljemalloc)
    SET(LINK_JEMALLOC "-L${CMAKE_BINARY_DIR}/build/lib -ljemalloc")
    ADD_DEPENDENCIES(shell jemalloc)
ELSE()
    SET(LINK_JEMALLOC "")
ENDIF()

target_link_libraries(
    topic
    PRIVATE taos pthread
)

target_include_directories(
    topic
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

SET_TARGET_PROPERTIES(topic PROPERTIES OUTPUT_NAME topic-producer)
