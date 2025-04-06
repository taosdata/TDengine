# if(${TD_LINUX})
aux_source_directory(src AZ_SRC)

add_library(az STATIC ${AZ_SRC})

if(${BUILD_S3})
  add_definitions(-DUSE_S3)
  DEP_ext_azure(az)
  DEP_ext_libxml2(az)
endif()

target_include_directories(
  az
  PUBLIC "${TD_SOURCE_DIR}/include/libs/azure"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
  az
  PUBLIC os
  PUBLIC util
  PUBLIC common
)

if(${BUILD_S3})
if(${BUILD_TEST})
  add_subdirectory(test)
endif(${BUILD_TEST})
endif()

# endif(${TD_LINUX})
