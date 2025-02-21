add_library(transport STATIC "")
set(
    TRANSPORT_SOURCE_FILES
    "src/thttp.c"
    "src/tmsgcb.c"
    "src/trans.c"
    "src/transCli.c"
    "src/transComm.c"
    "src/transSvr.c"
)
target_sources(
    transport
    PRIVATE
    ${TRANSPORT_SOURCE_FILES}
)
target_include_directories(
  transport
  PUBLIC "${TD_SOURCE_DIR}/include/libs/transport"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
  transport
  PUBLIC lz4_static
  PUBLIC os
  PUBLIC util
  PUBLIC common
  PUBLIC zlibstatic
)

if(${BUILD_WITH_UV_TRANS})
  if(${BUILD_WITH_UV})
    target_link_libraries(
      transport
      PUBLIC uv_a
    )
    add_definitions(-DUSE_UV)
  endif(${BUILD_WITH_UV})
endif(${BUILD_WITH_UV_TRANS})
