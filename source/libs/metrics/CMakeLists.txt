aux_source_directory(src METRICS_SRC)
add_library(metrics STATIC ${METRICS_SRC})
target_include_directories(
  metrics
  PUBLIC "${TD_SOURCE_DIR}/include/libs/metrics"
  PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(metrics os util common qcom transport)
