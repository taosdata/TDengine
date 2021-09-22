aux_source_directory(source OS_SRC)
add_library(os ${OS_SRC})
target_include_directories(
  os
  PUBLIC "${CMAKE_SOURCE_DIR}/include/os"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/include"
)