
INCLUDE(ExternalProject)

IF (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
    # freemine: IF (${TOOLS_BUILD_TYPE} MATCHES "Debug")
    # freemine:     MESSAGE("will not build jansson, use system's")
    # freemine: ELSE ()
    # freemine:     add_compile_options(-Wno-error=sign-compare)
    # freemine:     if(TD_ARM_32)
    # freemine:         MESSAGE("-- temporary change flag for snappy")
    # freemine:         IF (${CMAKE_BUILD_TYPE} MATCHES "Debug")
    # freemine:             set (TEMP_CMAKE_CXX_FLAGS_DEBUG ${CMAKE_CXX_FLAGS_DEBUG})
    # freemine:             string(REPLACE "-Wno-pointer-to-int-cast" "" CMAKE_CXX_FLAGS_DEBUG ${CMAKE_CXX_FLAGS_DEBUG})
    # freemine:             string(REPLACE "-Wno-incompatible-pointer-types" "" CMAKE_CXX_FLAGS_DEBUG ${CMAKE_CXX_FLAGS_DEBUG})
    # freemine:             set(CMAKE_CXX_FLAGS_DEBUG ${TEMP_CMAKE_CXX_FLAGS_DEBUG})
    # freemine:         ELSE ()
    # freemine:             set (TEMP_CMAKE_CXX_FLAGS_RELEASE ${CMAKE_CXX_FLAGS_RELEASE})
    # freemine:             string(REPLACE "-Wno-pointer-to-int-cast" "" CMAKE_CXX_FLAGS_RELEASE ${CMAKE_CXX_FLAGS_RELEASE})
    # freemine:             string(REPLACE "-Wno-incompatible-pointer-types" "" CMAKE_CXX_FLAGS_RELEASE ${CMAKE_CXX_FLAGS_RELEASE})
    # freemine:             set(CMAKE_CXX_FLAGS_RELEASE ${TEMP_CMAKE_CXX_FLAGS_RELEASE})
    # freemine:         ENDIF ()
    # freemine:     ENDIF()
    # freemine: ENDIF ()

    # freemine: SET(OS_ID "")
    # freemine: EXECUTE_PROCESS (
    # freemine:     COMMAND sh -c "awk -F= '/^ID=/{print $2}' /etc/os-release |tr -d '\n' | tr -d '\"'"
    # freemine:     OUTPUT_VARIABLE OS_ID
    # freemine:     )
    # freemine: MESSAGE(STATUS "deps' cmake detected current OS is " ${OS_ID} "")
    # freemine: ExternalProject_Add(
    # freemine:     deps-jansson
    # freemine:     PREFIX ${CMAKE_CURRENT_BINARY_DIR}/jansson
    # freemine:     SOURCE_DIR ${PROJECT_SOURCE_DIR}/deps/jansson
    # freemine:     CONFIGURE_COMMAND cmake -DJANSSON_BUILD_DOCS=OFF -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/build/ -DCMAKE_INSTALL_LIBDIR=lib -S ${PROJECT_SOURCE_DIR}/deps/jansson
    # freemine:     )

    # freemine: ExternalProject_Add(
    # freemine:     deps-snappy
    # freemine:     PREFIX ${CMAKE_CURRENT_BINARY_DIR}/snappy
    # freemine:     SOURCE_DIR ${PROJECT_SOURCE_DIR}/deps/snappy
    # freemine:     CONFIGURE_COMMAND cmake -DBUILD_SHARED_LIBS=OFF -DSNAPPY_BUILD_TESTS=OFF -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/build/ -DCMAKE_INSTALL_LIBDIR=lib -S ${PROJECT_SOURCE_DIR}/deps/snappy
    # freemine:     )

    # freemine: IF (${TOOLS_BUILD_TYPE} MATCHES "Debug")
    # freemine:     ExternalProject_Add(
    # freemine:         apache-avro
    # freemine:         PREFIX ${CMAKE_CURRENT_BINARY_DIR}/avro
    # freemine:         SOURCE_DIR ${PROJECT_SOURCE_DIR}/deps/avro
    # freemine:         CONFIGURE_COMMAND cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/build/ -DCMAKE_INSTALL_LIBDIR=lib -S ${PROJECT_SOURCE_DIR}/deps/avro/lang/c
    # freemine:         )
    # freemine: ELSE ()
    # freemine:     ExternalProject_Add(
    # freemine:         apache-avro
    # freemine:         PREFIX ${CMAKE_CURRENT_BINARY_DIR}/avro
    # freemine:         SOURCE_DIR ${PROJECT_SOURCE_DIR}/deps/avro
    # freemine:         CONFIGURE_COMMAND cmake -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/build/ -DCMAKE_INSTALL_LIBDIR=lib -S ${PROJECT_SOURCE_DIR}/deps/avro/lang/c
    # freemine:         )
    # freemine: ENDIF ()
ELSEIF(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
ELSEIF(${CMAKE_SYSTEM_NAME} MATCHES "Windows")
    # freemine: IF (${TOOLS_BUILD_TYPE} MATCHES "Debug")
    # freemine:     MESSAGE("will not build jansson, use system's")
    # freemine: ELSE ()
    # freemine:     # add_compile_options(-Wno-error=sign-compare)
    # freemine:     if(TD_ARM_32)
    # freemine:         MESSAGE("-- temporary change flag for snappy")
    # freemine:         IF (${CMAKE_BUILD_TYPE} MATCHES "Debug")
    # freemine:             set (TEMP_CMAKE_CXX_FLAGS_DEBUG ${CMAKE_CXX_FLAGS_DEBUG})
    # freemine:             string(REPLACE "-Wno-pointer-to-int-cast" "" CMAKE_CXX_FLAGS_DEBUG ${CMAKE_CXX_FLAGS_DEBUG})
    # freemine:             string(REPLACE "-Wno-incompatible-pointer-types" "" CMAKE_CXX_FLAGS_DEBUG ${CMAKE_CXX_FLAGS_DEBUG})
    # freemine:             set(CMAKE_CXX_FLAGS_DEBUG ${TEMP_CMAKE_CXX_FLAGS_DEBUG})
    # freemine:         ELSE ()
    # freemine:             set (TEMP_CMAKE_CXX_FLAGS_RELEASE ${CMAKE_CXX_FLAGS_RELEASE})
    # freemine:             string(REPLACE "-Wno-pointer-to-int-cast" "" CMAKE_CXX_FLAGS_RELEASE ${CMAKE_CXX_FLAGS_RELEASE})
    # freemine:             string(REPLACE "-Wno-incompatible-pointer-types" "" CMAKE_CXX_FLAGS_RELEASE ${CMAKE_CXX_FLAGS_RELEASE})
    # freemine:             set(CMAKE_CXX_FLAGS_RELEASE ${TEMP_CMAKE_CXX_FLAGS_RELEASE})
    # freemine:         ENDIF ()
    # freemine:     ENDIF()
    # freemine: ENDIF ()

    # freemine: MESSAGE(STATUS "deps' cmake detected current OS is Windows")
    # freemine: ExternalProject_Add(
    # freemine:     tools-zlib
    # freemine:     PREFIX ${CMAKE_CURRENT_BINARY_DIR}/tools-zlib
    # freemine:     SOURCE_DIR ${PROJECT_SOURCE_DIR}/deps/zlib-1.2.13
    # freemine:     CONFIGURE_COMMAND cmake -G "NMake Makefiles JOM" -DCMAKE_MAKE_PROGRAM=jom -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/build/ -S ${PROJECT_SOURCE_DIR}/deps/zlib-1.2.13
    # freemine:     )
    # freemine: ExternalProject_Add(
    # freemine:     deps-jansson
    # freemine:     PREFIX ${CMAKE_CURRENT_BINARY_DIR}/jansson
    # freemine:     SOURCE_DIR ${PROJECT_SOURCE_DIR}/deps/jansson
    # freemine:     CONFIGURE_COMMAND cmake -G "NMake Makefiles JOM" -DCMAKE_MAKE_PROGRAM=jom -DJANSSON_BUILD_DOCS=OFF -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/build/ -S ${PROJECT_SOURCE_DIR}/deps/jansson
    # freemine:     PATCH_COMMAND
    # freemine:     COMMAND cmake -E copy ${PROJECT_SOURCE_DIR}/deps/jansson-value.c ${PROJECT_SOURCE_DIR}/deps/jansson/src/value.c
    # freemine:     )

    # freemine: libargp moved to cmake/externals.cmake
    add_subdirectory(emfisis.physics.uiowa.edu/Software/C/libargp)

    # freemine: ExternalProject_Add(
    # freemine:     deps-snappy
    # freemine:     PREFIX ${CMAKE_CURRENT_BINARY_DIR}/snappy
    # freemine:     SOURCE_DIR ${PROJECT_SOURCE_DIR}/deps/snappy
    # freemine:     CONFIGURE_COMMAND cmake -G "NMake Makefiles JOM" -DCMAKE_MAKE_PROGRAM=jom -DBUILD_SHARED_LIBS=OFF -DSNAPPY_BUILD_TESTS=OFF -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/build/ -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -S ${PROJECT_SOURCE_DIR}/deps/snappy
    # freemine:     )

    IF (${TOOLS_BUILD_TYPE} MATCHES "Debug")
        SET(BUILD_TYPE "-DCMAKE_BUILD_TYPE=Debug")
    ELSE ()
        SET(BUILD_TYPE "")
    ENDIF ()
    # freemine: ExternalProject_Add(
    # freemine:     apache-avro
    # freemine:     PREFIX ${CMAKE_CURRENT_BINARY_DIR}/avro
    # freemine:     SOURCE_DIR ${PROJECT_SOURCE_DIR}/deps/avro
    # freemine:     PATCH_COMMAND
    # freemine:     COMMAND sed -i "s/^install(TARGETS avro[^-]/# install(TARGETS avro/g" ${PROJECT_SOURCE_DIR}/deps/avro/lang/c/src/CMakeLists.txt
    # freemine:     COMMAND sed -i "s/^target_link_libraries(avro[^-]/# target_link_libraries(avro/g" ${PROJECT_SOURCE_DIR}/deps/avro/lang/c/src/CMakeLists.txt
    # freemine:     COMMAND sed -i "s/^add_executable(avro/# add_executable(avro/g" ${PROJECT_SOURCE_DIR}/deps/avro/lang/c/src/CMakeLists.txt
    # freemine:     COMMAND sed -i "s/^#include <stdlib.h>/#define ssize_t int\\n #include <stdlib.h>/g" ${PROJECT_SOURCE_DIR}/deps/avro/lang/c/src/generic.c
    # freemine:     COMMAND sed -i "s/__cdecl//g" ${PROJECT_SOURCE_DIR}/deps/avro/lang/c/src/avro/msinttypes.h
    # freemine:     COMMAND sed -i "s/imaxdiv_t/imaxdiv1_t/g" ${PROJECT_SOURCE_DIR}/deps/avro/lang/c/src/avro/msinttypes.h
    # freemine:     COMMAND sed -i "s/imaxdiv(/imax_div(/g" ${PROJECT_SOURCE_DIR}/deps/avro/lang/c/src/avro/msinttypes.h
    # freemine:     COMMAND sed -i "s/typedef uint16_t  uint_fast16_t//g" ${PROJECT_SOURCE_DIR}/deps/avro/lang/c/src/avro/msstdint.h
    # freemine:     COMMAND sed -i "s/typedef int16_t   int_fast16_t//g" ${PROJECT_SOURCE_DIR}/deps/avro/lang/c/src/avro/msstdint.h
    # freemine:     COMMAND sed -i "s/#define snprintf _snprintf//g" ${PROJECT_SOURCE_DIR}/deps/avro/lang/c/src/avro_private.h
    # freemine:     COMMAND cmake -E copy ${PROJECT_SOURCE_DIR}/deps/avro-lang-c-CMakeLists.txt ${PROJECT_SOURCE_DIR}/deps/avro/lang/c/CMakeLists.txt
    # freemine:     COMMAND cmake -E copy ${PROJECT_SOURCE_DIR}/deps/avro-lang-c-src-codec.c ${PROJECT_SOURCE_DIR}/deps/avro/lang/c/src/codec.c
    # freemine:     COMMAND cmake -E copy ${PROJECT_SOURCE_DIR}/deps/avro-lang-c-src-schema.c ${PROJECT_SOURCE_DIR}/deps/avro/lang/c/src/schema.c
    # freemine:     CONFIGURE_COMMAND cmake -G "NMake Makefiles JOM" ${BUILD_TYPE} -DCMAKE_MAKE_PROGRAM=jom -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/build/ -S ${PROJECT_SOURCE_DIR}/deps/avro/lang/c
    # freemine:     )
    # freemine: #    ADD_SUBDIRECTORY(CUnit)
ENDIF()
ADD_SUBDIRECTORY(toolscJson)
