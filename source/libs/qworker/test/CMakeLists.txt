
MESSAGE(STATUS "build qworker unit test")

IF(NOT TD_DARWIN)
        # GoogleTest requires at least C++11
        if(${FALLBACK})          # {
            SET(CMAKE_CXX_STANDARD 11)
        else()                   # }{
            SET(CMAKE_CXX_STANDARD 14)
        endif()                  # }
        AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

        ADD_EXECUTABLE(qworkerTest ${SOURCE_LIST})
        if(${FALLBACK})                     # {
                TARGET_LINK_LIBRARIES(
                        qworkerTest
                        PUBLIC os util common transport gtest qcom nodes planner qworker executor index
                )
        else()                              # }{
                TARGET_LINK_LIBRARIES(
                        qworkerTest
                        PUBLIC os util common transport qcom nodes planner qworker executor index
                )
                DEP_ext_gtest(qworkerTest)
                DEP_ext_stub(qworkerTest)
                target_include_directories(qworkerTest
                        PUBLIC ${ext_stub_stub} ${ext_stub_addr_any}
                )
        endif()                             # }

        TARGET_INCLUDE_DIRECTORIES(
                qworkerTest
                PUBLIC "${TD_SOURCE_DIR}/include/libs/qworker/"
                PRIVATE "${TD_SOURCE_DIR}/source/libs/qworker/inc"
        )
ENDIF()
