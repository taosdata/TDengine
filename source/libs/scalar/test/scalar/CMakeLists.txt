
MESSAGE(STATUS "build scalar unit test")

IF(NOT TD_DARWIN)
        # GoogleTest requires at least C++11
        if(${FALLBACK})          # {
            SET(CMAKE_CXX_STANDARD 11)
        else()                   # }{
            SET(CMAKE_CXX_STANDARD 14)
        endif()                  # }
        AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

        ADD_EXECUTABLE(scalarTest ${SOURCE_LIST})
        if(${FALLBACK})                        # {
                TARGET_LINK_LIBRARIES(
                        scalarTest
                        PUBLIC os util common gtest qcom function nodes scalar parser catalog transport
                )
        else()                                 # }{
                TARGET_LINK_LIBRARIES(
                        scalarTest
                        PUBLIC os util common qcom function nodes scalar parser catalog transport
                )
                DEP_ext_gtest(scalarTest)
                DEP_ext_stub(scalarTest)
                target_include_directories(scalarTest
                        PUBLIC ${ext_stub_stub} ${ext_stub_addr_any}
                )
        endif()                                # }

        TARGET_INCLUDE_DIRECTORIES(
                scalarTest
                PUBLIC "${TD_SOURCE_DIR}/include/libs/scalar/"
                PUBLIC "${TD_SOURCE_DIR}/source/libs/parser/inc"
                PRIVATE "${TD_SOURCE_DIR}/source/libs/scalar/inc"
        )
        add_test(
                NAME scalarTest
                COMMAND scalarTest
        )
ENDIF()
