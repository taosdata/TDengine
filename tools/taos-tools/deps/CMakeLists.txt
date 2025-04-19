
INCLUDE(ExternalProject)

IF(${CMAKE_SYSTEM_NAME} MATCHES "Windows")
    # NOTE: libargp moved to cmake/externals.cmake
    add_subdirectory(emfisis.physics.uiowa.edu/Software/C/libargp)

    IF (${TOOLS_BUILD_TYPE} MATCHES "Debug")
        SET(BUILD_TYPE "-DCMAKE_BUILD_TYPE=Debug")
    ELSE ()
        SET(BUILD_TYPE "")
    ENDIF ()
ENDIF()
ADD_SUBDIRECTORY(toolscJson)
