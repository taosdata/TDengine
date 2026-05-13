include(${CMAKE_CURRENT_LIST_DIR}/Utils.cmake)
setup_colored_output()

# Set search paths based on platform
if(WIN32)
    set(TAOS_SEARCH_PATHS
        "C:/TDengine/driver"
        "C:/TDengine/include"
    )
    set(TAOS_LIB_NAMES "taos.dll" "taos.lib")
else()
    set(TAOS_SEARCH_PATHS
        "/usr/local/lib"
        "/usr/local/taos"
        "/usr/local"
        "/usr"
    )
    set(TAOS_LIB_NAMES "libtaos.so" "libtaos.dylib")
endif()

# Find TDengine header
find_path(TAOS_INCLUDE_DIR
    NAMES taos.h
    PATHS ${TAOS_SEARCH_PATHS}
    PATH_SUFFIXES include include/taos
    DOC "TDengine header directory"
)

# Find TDengine library
find_library(TAOS_LIBRARY
    NAMES ${TAOS_LIB_NAMES}
    PATHS ${TAOS_SEARCH_PATHS}
    PATH_SUFFIXES lib lib64
    DOC "TDengine library"
)

if(NOT TAOS_LIBRARY)
    foreach(libname ${TAOS_LIB_NAMES})
        foreach(path ${TAOS_SEARCH_PATHS})
            set(test_path "${path}/${libname}")
            if(EXISTS ${test_path})
                get_filename_component(real_path ${test_path} REALPATH)
                if(EXISTS ${real_path})
                    set(TAOS_LIBRARY ${real_path})
                    break()
                endif()
            endif()
        endforeach()
        if(TAOS_LIBRARY)
            break()
        endif()
    endforeach()
endif()

# Version check
if(TAOS_INCLUDE_DIR AND TAOS_LIBRARY)
    message(STATUS "TAOS_INCLUDE_DIR: ${TAOS_INCLUDE_DIR}")
    message(STATUS "TAOS_LIBRARY: ${TAOS_LIBRARY}")

    include(CheckFunctionExists)
    set(CMAKE_REQUIRED_LIBRARIES ${TAOS_LIBRARY})
    set(CMAKE_REQUIRED_INCLUDES ${TAOS_INCLUDE_DIR})
    check_function_exists(taos_query HAVE_TAOS_QUERY)

    if(NOT HAVE_TAOS_QUERY)
        message(FATAL_ERROR "${Red}Found TDengine files but symbols are missing${ColorReset}")
    endif()
endif()

# Handle REQUIRED and QUIET arguments
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(TAOS
    REQUIRED_VARS
        TAOS_LIBRARY
        TAOS_INCLUDE_DIR
        HAVE_TAOS_QUERY
    FAIL_MESSAGE "${Red}Could not find TDengine client library${ColorReset}"
)

if(TAOS_FOUND)
    set(TAOS_LIBRARIES ${TAOS_LIBRARY})
    set(TAOS_INCLUDE_DIRS ${TAOS_INCLUDE_DIR})

    if(NOT TARGET TAOS::TAOS)
        add_library(TAOS::TAOS UNKNOWN IMPORTED)
        set_target_properties(TAOS::TAOS PROPERTIES
            IMPORTED_LOCATION "${TAOS_LIBRARY}"
            INTERFACE_INCLUDE_DIRECTORIES "${TAOS_INCLUDE_DIR}"
        )
    endif()

    message(STATUS "${Green}Found TDengine: ${TAOS_LIBRARY}${ColorReset}")
endif()

mark_as_advanced(TAOS_INCLUDE_DIR TAOS_LIBRARY)