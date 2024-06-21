aux_source_directory(src MONITOR_SRC)
add_library(monitor STATIC ${MONITOR_SRC})
target_include_directories(
  monitor
  PUBLIC "${TD_SOURCE_DIR}/include/libs/monitor"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(monitor os util common qcom transport monitorfw)

if(${BUILD_TEST})
    add_subdirectory(test)
endif(${BUILD_TEST})