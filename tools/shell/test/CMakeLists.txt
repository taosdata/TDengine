
MESSAGE(STATUS "build taos-CLI unit test")

IF(NOT TD_DARWIN)
        # GoogleTest requires at least C++11
        if(${FALLBACK})          # {
            SET(CMAKE_CXX_STANDARD 11)
        else()                   # }{
            SET(CMAKE_CXX_STANDARD 14)
        endif()                  # }
        AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

        ADD_EXECUTABLE(shellTest ${SOURCE_LIST})
        if(${FALLBACK})                 # {
            TARGET_LINK_LIBRARIES(
                    shellTest
                    PRIVATE shell_ut gtest os common transport geometry util
            )
        else()                          # }{
            TARGET_LINK_LIBRARIES(
                    shellTest
                    PUBLIC shell_ut os common transport geometry util
            )
            DEP_ext_gtest(shellTest)
        endif()                         # }

        target_include_directories(
            shell_ut
            PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
        )
        

        add_test(
            NAME shellTest
            COMMAND shellTest
        )
ENDIF()
