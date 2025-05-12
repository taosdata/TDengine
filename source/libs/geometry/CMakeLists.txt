aux_source_directory(src GEOMETRY_SRC)

add_library(geometry STATIC ${GEOMETRY_SRC})
target_include_directories(
  geometry
  PUBLIC "${TD_SOURCE_DIR}/include/libs/geometry"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../scalar/inc"
)

target_link_libraries(
  geometry
  PRIVATE os util nodes function scalar qcom
)

if(${BUILD_TEST})
  ADD_SUBDIRECTORY(test)
endif(${BUILD_TEST})
