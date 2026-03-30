# Generate API documentation
## https://vicrucann.github.io/tutorials/quick-cmake-doxygen/
if(${BUILD_DOCS})
    find_package(Doxygen)
    if (DOXYGEN_FOUND)
        # Build the doc
        set(DOXYGEN_IN ${TD_SOURCE_DIR}/docs/doxgen/Doxyfile.in)
        set(DOXYGEN_OUT ${CMAKE_BINARY_DIR}/Doxyfile)

        configure_file(${DOXYGEN_IN} ${DOXYGEN_OUT} @ONLY)
        message("Doxygen build start")

        add_custom_target(
            tdengine_doxygen ALL
            COMMAND ${DOXYGEN_EXECUTABLE} ${DOXYGEN_OUT}
            WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
            COMMENT "Generating API doxumentation with Doxygen"
            VERBATIM
        )
    else(DOXYGEN_FOUND)
        message("Doxygen need to be installed to generate the doxygen documentation")
    endif(DOXYGEN_FOUND)
endif(${BUILD_DOCS})