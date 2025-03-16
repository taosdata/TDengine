aux_source_directory(src TRANSPORT_SRC)
add_library(transport STATIC ${TRANSPORT_SRC})
if(${BUILD_WITH_UV_TRANS})
  if(${BUILD_WITH_UV})
		DEP_ext_libuv(transport)
	endif()
endif()
target_include_directories(
  transport
  PUBLIC "${TD_SOURCE_DIR}/include/libs/transport"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
  transport
  PUBLIC os
  PUBLIC util
  PUBLIC common
)

if(${BUILD_WITH_UV_TRANS})
  if(${BUILD_WITH_UV})
    add_definitions(-DUSE_UV)
  endif(${BUILD_WITH_UV})
endif(${BUILD_WITH_UV_TRANS})
