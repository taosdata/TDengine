aux_source_directory(src METRIC_SRC)
add_library(tmetrics STATIC ${METRIC_SRC})
target_include_directories(
  tmetrics
  PUBLIC "${CMAKE_SOURCE_DIR}/include/libs/metric"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(tmetrics os util common qcom transport)

if(${BUILD_TEST})
  add_subdirectory(test)
endif(${BUILD_TEST})

IF (TD_LINUX)
  SET_TARGET_PROPERTIES(tmetrics PROPERTIES CLEAN_DIRECT_OUTPUT 1)
  TARGET_LINK_LIBRARIES(tmetrics pthread m rt)
ENDIF ()

IF (TD_DARWIN)
  SET_TARGET_PROPERTIES(tmetrics PROPERTIES CLEAN_DIRECT_OUTPUT 1)
  TARGET_LINK_LIBRARIES(tmetrics pthread m)
ENDIF ()

INSTALL(FILES inc/tmetrics.h DESTINATION include/libs/metric)

INSTALL(TARGETS tmetrics
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib) 