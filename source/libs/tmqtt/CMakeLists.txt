aux_source_directory(src/xnode_mgmt_mqtt XNODE_MGMT_MQTT_SRC)
aux_source_directory(src TAOSMQTT_SRC)
aux_source_directory(lib TMQTT_SRC)

# _xnode_mgmt_mqtt
add_library(_xnode_mgmt_mqtt STATIC ${XNODE_MGMT_MQTT_SRC})

target_include_directories(
    _xnode_mgmt_mqtt
    PUBLIC
    "${TD_SOURCE_DIR}/include/libs/tmqtt"
    "${TD_SOURCE_DIR}/include/util"
    "${TD_SOURCE_DIR}/include/common"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
    _xnode_mgmt_mqtt
    PRIVATE os util common nodes transport
)

# taosmqtt
if(TD_LINUX)
find_path(HAVE_SYS_EPOLL_H sys/epoll.h)
if (HAVE_SYS_EPOLL_H)
	add_definitions("-DWITH_EPOLL")
endif()


add_executable(taosmqtt ${TAOSMQTT_SRC}
  lib/ttqNet.c
  lib/packetDataTypes.c
  lib/ttqPacket.c
  lib/ttqSend.c
  lib/sendPublish.c
  lib/ttqProperty.c
  lib/ttqMemory.c
  lib/ttqMisc.c
  lib/ttqUtil.c
  lib/ttqTime.c
  lib/ttqStrings.c
  lib/ttqUtf8.c
  lib/utilTopic.c
)

DEP_ext_cjson(taosmqtt)
DEP_ext_libuv(taosmqtt)

target_compile_definitions(taosmqtt PRIVATE WITH_BROKER)

if(${TD_DARWIN})
    target_compile_options(taosmqtt PRIVATE -Wno-error=deprecated-non-prototype)
endif()

target_include_directories(
    taosmqtt
    PUBLIC
    "${TD_SOURCE_DIR}/include/libs/"
    "${TD_SOURCE_DIR}/include/util"
    "${TD_SOURCE_DIR}/include/common"
    "${TD_SOURCE_DIR}/include/libs/transport"
    "${TD_SOURCE_DIR}/include/client"
    "${TD_SOURCE_DIR}/include/dnode/xnode"
    PRIVATE "${TD_SOURCE_DIR}/include/libs/tmqtt"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/lib/inc"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/src/inc"
)

target_link_libraries(
    taosmqtt
    PRIVATE util common ${TAOS_NATIVE_LIB_STATIC}
)

# topic tool
aux_source_directory(tools TOPICTOOL_SRC)

add_executable(topic-test-tool ${TOPICTOOL_SRC})

target_link_libraries(
    topic-test-tool
    PRIVATE pthread util common ${TAOS_NATIVE_LIB_STATIC}
)

target_include_directories(
    topic-test-tool
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

SET_TARGET_PROPERTIES(topic-test-tool PROPERTIES OUTPUT_NAME topic-producer)
endif()

# test
if(${BUILD_TEST})
    #add_subdirectory(test)
endif()

