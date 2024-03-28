aux_source_directory(src MONITOR2_SRC)
add_library(monitorfw STATIC ${MONITOR2_SRC})
target_include_directories(
  monitorfw
  PUBLIC "${TD_SOURCE_DIR}/include/libs/monitorfw"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(monitorfw os util common transport)
