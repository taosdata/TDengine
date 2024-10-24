aux_source_directory(src MONITOR2_SRC)
add_library(monitorfw STATIC ${MONITOR2_SRC})
target_include_directories(
  monitorfw
  PUBLIC "${TD_SOURCE_DIR}/include/libs/monitorfw"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

if(${TD_DARWIN})
  target_compile_options(monitorfw PRIVATE -Wno-error=deprecated-pragma)
endif(${TD_DARWIN})

target_link_libraries(monitorfw os util common transport)
