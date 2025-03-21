aux_source_directory(src TAOSMQTT_SRC)
aux_source_directory(src/xnode_mgmt_mqtt XNODE_MGMT_MQTT_SRC)

message("xxx" ${TAOSMQTT_SRC})
message("xxx2" ${XNODE_MGMT_MQTT_SRC})
#list(REMOVE_ITEM FUNCTION_SRC src/mqttd.c)

if(COMPILER_SUPPORT_AVX2)
    MESSAGE(STATUS "AVX2 instructions is ACTIVATED")
    set_source_files_properties(src/detail/tminmaxavx.c PROPERTIES COMPILE_FLAGS -mavx2)
endif()

#add_library(_xnode_mgmt_mqtt STATIC ${XNODE_MGMT_MQTT_SRC})
#
#target_include_directories(
#    _xnode_mgmt_mqtt
#    PUBLIC
#    "${TD_SOURCE_DIR}/include/libs/tmqtt"
#    "${TD_SOURCE_DIR}/include/util"
#    "${TD_SOURCE_DIR}/include/common"
#    "${TD_SOURCE_DIR}/include/client"
#    "${TD_SOURCE_DIR}/contrib/libuv/include"
#    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
#)
#
#target_link_libraries(
#    _xnode_mgmt_mqtt
#    PRIVATE os
#    PRIVATE util
#    PRIVATE common
#    PRIVATE nodes
#    PRIVATE transport
#    PUBLIC uv_a
#)

#add_executable(taosmqtt ${TAOSMQTT_SRC})
#
#if(${TD_DARWIN})
#    target_compile_options(taosmqtt PRIVATE -Wno-error=deprecated-non-prototype)
#endif()
#
#target_include_directories(
#    taosmqtt
#    PUBLIC
#    "${TD_SOURCE_DIR}/include/libs/tmqtt"
#    "${TD_SOURCE_DIR}/contrib/libuv/include"
#    "${TD_SOURCE_DIR}/include/util"
#    "${TD_SOURCE_DIR}/include/common"
#    "${TD_SOURCE_DIR}/include/libs/transport"
#    "${TD_SOURCE_DIR}/include/client"
#    "${TD_SOURCE_DIR}/include/libs/tmqtt"
#    "${TD_SOURCE_DIR}/include/dnode/xnode"
#    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
#)
#
#target_link_libraries(
#    taosmqtt
#    PRIVATE os util common
#)

if(${BUILD_TEST})
    #add_subdirectory(test)
endif()

#aux_source_directory(src TAOSM_SRC)
#
#add_executable(mqttd ${TAOSM_SRC})
#
#IF(TD_LINUX_64 AND JEMALLOC_ENABLED)
#    ADD_DEFINITIONS(-DTD_JEMALLOC_ENABLED -I${CMAKE_BINARY_DIR}/build/include -L${CMAKE_BINARY_DIR}/build/lib -Wl,-rpath,${CMAKE_BINARY_DIR}/build/lib -ljemalloc)
#    SET(LINK_JEMALLOC "-L${CMAKE_BINARY_DIR}/build/lib -ljemalloc")
#    ADD_DEPENDENCIES(shell jemalloc)
#ELSE()
#    SET(LINK_JEMALLOC "")
#ENDIF()
#
#target_link_libraries(
#    mqttd
#    PRIVATE taos pthread
#)
#
#target_include_directories(
#    mqttd
#    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
#)
#
#SET_TARGET_PROPERTIES(mqttd PROPERTIES OUTPUT_NAME mqttd)
#
## topic tool
#aux_source_directory(tools TOPICTOOL_SRC)
#
#add_executable(topic ${TOPICTOOL_SRC})
#
#IF(TD_LINUX_64 AND JEMALLOC_ENABLED)
#    ADD_DEFINITIONS(-DTD_JEMALLOC_ENABLED -I${CMAKE_BINARY_DIR}/build/include -L${CMAKE_BINARY_DIR}/build/lib -Wl,-rpath,${CMAKE_BINARY_DIR}/build/lib -ljemalloc)
#    SET(LINK_JEMALLOC "-L${CMAKE_BINARY_DIR}/build/lib -ljemalloc")
#    ADD_DEPENDENCIES(shell jemalloc)
#ELSE()
#    SET(LINK_JEMALLOC "")
#ENDIF()
#
#target_link_libraries(
#    topic
#    PRIVATE taos pthread
#)
#
#target_include_directories(
#    topic
#    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
#)
#
#SET_TARGET_PROPERTIES(topic PROPERTIES OUTPUT_NAME topic-producer)
