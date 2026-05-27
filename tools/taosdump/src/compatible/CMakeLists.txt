# AVRO compatibility import module for taosBackup
# Builds a static library that reads taosdump AVRO backup files

SET(COMPAT_AVRO_SRCS
    compatAvro.c
    compatAvroSchema.c
    compatAvroMeta.c
    compatAvroData.c
    compatAvroUtil.c
)

ADD_LIBRARY(compatAvro STATIC ${COMPAT_AVRO_SRCS})

# Include paths
target_include_directories(compatAvro PRIVATE
    ${CMAKE_BINARY_DIR}/build/include       # TDengine headers (taos.h, tdef.h, etc.)
    ${CMAKE_CURRENT_SOURCE_DIR}/../../inc      # taosBackup headers (bck.h, bckLog.h, etc.)
    ${CMAKE_CURRENT_SOURCE_DIR}             # compatAvro internal headers
)

# TDengine client library (for include paths and type definitions)
target_link_libraries(compatAvro PUBLIC taos)

# AVRO + JSON dependencies (provided by conan via DEP_ext_* macros)
DEP_ext_avro(compatAvro)
DEP_ext_jansson(compatAvro)
DEP_ext_snappy(compatAvro)

# libavro requires liblzma for LZMA codec support.
# Use the project's own ext_xz (liblzma.a built from source) instead of the
# system library so the build works in the CI container where liblzma-devel
# is not installed.
DEP_ext_xz(compatAvro)

# Hide symbols to avoid conflicts with Arrow's bundled snappy/zstd
if(NOT WIN32)
    target_compile_options(compatAvro PRIVATE -fvisibility=hidden)
endif()
