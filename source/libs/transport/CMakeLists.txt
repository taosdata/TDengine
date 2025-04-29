aux_source_directory(src TRANSPORT_SRC)
add_library(transport STATIC ${TRANSPORT_SRC})
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
    DEP_ext_libuv(transport)
    if(TD_PTHREAD_TWEAK)       # {
      target_link_libraries(transport PRIVATE pthread)
    endif()                    # }
  endif()
endif()

if(${BUILD_WITH_UV_TRANS})
  if(${BUILD_WITH_UV})
    add_definitions(-DUSE_UV)
  endif(${BUILD_WITH_UV})
endif(${BUILD_WITH_UV_TRANS})
