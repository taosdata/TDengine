
MESSAGE(STATUS "build taos-CLI unit test")

IF(NOT TD_DARWIN)
        # GoogleTest requires at least C++11
        SET(CMAKE_CXX_STANDARD 11)
        AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

        ADD_EXECUTABLE(shellTest ${SOURCE_LIST})
        DEP_ext_gtest(shellTest)
        TARGET_LINK_LIBRARIES(
                shellTest
                PRIVATE shell_ut os common transport geometry util
        )

        target_include_directories(
            shell_ut
            PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
        )
        

        add_test(
            NAME shellTest
            COMMAND shellTest
        )
ENDIF()
