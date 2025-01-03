aux_source_directory(src TRANSPORT_SRC)
add_library(transport STATIC ${TRANSPORT_SRC})
add_dependencies(transport ext_libuv)
target_include_directories(transport PUBLIC "${TD_CONTRIB_EXT_LIBUV_INC_DIR}")
target_link_directories(transport PUBLIC "${TD_CONTRIB_EXT_LIBUV_LIB_DIR}")
target_include_directories(
  transport
  PUBLIC "${TD_SOURCE_DIR}/include/libs/transport"
  PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
  transport
  PUBLIC os
  PUBLIC util
  PUBLIC common
)

if(${BUILD_WITH_UV_TRANS})
  if(${BUILD_WITH_UV})
    target_link_libraries(
      transport
      PUBLIC uv.a
    )
    add_definitions(-DUSE_UV)
  endif(${BUILD_WITH_UV})
endif(${BUILD_WITH_UV_TRANS})
