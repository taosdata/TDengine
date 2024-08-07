aux_source_directory(src SM4_SRC)
add_library(sm4 STATIC ${SM4_SRC})
target_include_directories(
  sm4
  PUBLIC "${TD_SOURCE_DIR}/include/libs/sm4"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(sm4 common)
