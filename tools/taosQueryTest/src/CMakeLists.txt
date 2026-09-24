add_library(taosQueryTestCore STATIC
  qst_crc32c.c
  qst_shm.c
  qst_block.c
  qst_queue.c
  qst_task.c
  qst_process.c
  qst_config.c
  qst_resource.c
  qst_sha256.c
  qst_supervisor.c
  qst_native.c
  qst_result.c
  qst_result_stream.c
  qst_result_store.c
  qst_report.c
  qst_ai.c
  qst_ai_runner.c
  qst_audit.c
  qst_type_registry.c
  qst_dataset.c
  qst_generator.c
  qst_mapping.c
  qst_provider.c
  qst_runner.c
  sql_dimension_catalog_source.c
)

target_include_directories(taosQueryTestCore
  PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
  PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/../../../include/client"
)

target_compile_features(taosQueryTestCore PRIVATE c_std_11)

if(TD_LINUX)
  target_link_libraries(taosQueryTestCore PUBLIC pthread rt dl)
endif()

target_link_libraries(taosQueryTestCore PRIVATE toolscJson)

add_executable(taosQueryTest taosQueryTest.c)
target_link_libraries(taosQueryTest PRIVATE taosQueryTestCore)
target_include_directories(taosQueryTest PRIVATE
  "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
  "${CMAKE_CURRENT_SOURCE_DIR}"
)
