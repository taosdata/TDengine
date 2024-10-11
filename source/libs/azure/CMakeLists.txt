#if(${TD_LINUX})
aux_source_directory(src AZ_SRC)

#add_library(az STATIC ${AZ_SRC})
add_library(az STATIC
  src/az.cpp
  )

if(${BUILD_S3})
  add_definitions(-DUSE_S3)
  target_link_libraries(
    az
    #PUBLIC _azure_sdk
    PUBLIC crypt
  )
endif()

target_include_directories(
    az
    PUBLIC "${TD_SOURCE_DIR}/include/libs/azure"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
    az
    PUBLIC cjson
    PUBLIC os
    PUBLIC util
    PUBLIC common
)

if(${BUILD_TEST})
    add_subdirectory(test)
endif(${BUILD_TEST})
#endif(${TD_LINUX})
