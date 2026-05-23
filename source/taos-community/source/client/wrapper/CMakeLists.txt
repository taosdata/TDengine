aux_source_directory(src WRAPPER_SRC)

if(TD_WINDOWS)
    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/../src/taos.rc.in "${CMAKE_CURRENT_BINARY_DIR}/taos.rc")
    add_library(${TAOS_LIB} SHARED ${WRAPPER_SRC} "${CMAKE_CURRENT_BINARY_DIR}/taos.rc")
else()
    add_library(${TAOS_LIB} SHARED ${WRAPPER_SRC})
endif()

if(TD_DARWIN)
    target_compile_options(${TAOS_LIB} PRIVATE -Wno-error=deprecated-non-prototype)
    # Suppress ld64 duplicate static library warnings from deep transitive deps.
    include(CheckLinkerFlag)
    check_linker_flag(C "-Wl,-no_warn_duplicate_libraries" HAVE_NO_WARN_DUP_LIBS)
    if(HAVE_NO_WARN_DUP_LIBS)
      target_link_options(${TAOS_LIB} PRIVATE -Wl,-no_warn_duplicate_libraries)
    endif()
endif()

# jni include
INCLUDE_DIRECTORIES(jni)
if(TD_WINDOWS)
    INCLUDE_DIRECTORIES(jni/windows)
    INCLUDE_DIRECTORIES(jni/windows/win32)
    INCLUDE_DIRECTORIES(jni/windows/win32/bridge)
else()
    INCLUDE_DIRECTORIES(jni/linux)
endif()

target_include_directories(
    ${TAOS_LIB}
    PUBLIC "${TD_SOURCE_DIR}/include/client"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
    ${TAOS_LIB}
    PUBLIC os util
)

add_library(${TAOS_LIB_STATIC} STATIC ${WRAPPER_SRC})

if(TD_DARWIN)
    target_compile_options(${TAOS_LIB_STATIC} PRIVATE -Wno-error=deprecated-non-prototype)
endif()

target_include_directories(
    ${TAOS_LIB_STATIC}
    PUBLIC "${TD_SOURCE_DIR}/include/client"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
    ${TAOS_LIB_STATIC}
    PUBLIC os util
)

# if(${BUILD_TEST})
# ADD_SUBDIRECTORY(test)
# endif()
