aux_source_directory(src RATE_LIMIT_SRC)

add_library(rate_limit STATIC ${RATE_LIMIT_SRC})

target_include_directories(
  rate_limit
  PUBLIC "${TD_SOURCE_DIR}/source/libs/rate_limit/inc"
  PUBLIC "${TD_SOURCE_DIR}/include/os"
  PUBLIC "${TD_SOURCE_DIR}/include/util"
)

if(TD_WINDOWS)
  target_link_libraries(rate_limit PRIVATE ws2_32)
endif()
