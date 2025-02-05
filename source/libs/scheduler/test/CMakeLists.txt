
MESSAGE(STATUS "build scheduler unit test")

IF(NOT TD_DARWIN)
        # GoogleTest requires at least C++11
        SET(CMAKE_CXX_STANDARD 11)
        AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

        ADD_EXECUTABLE(schedulerTest ${SOURCE_LIST})

        IF (TD_GRANT)
                TARGET_LINK_LIBRARIES(
                        schedulerTest
                        PUBLIC os util common catalog transport gtest qcom ${TAOS_LIB_STATIC} planner scheduler grant
                )
        ELSE ()
                if(${FALLBACK})                  # {
                        TARGET_LINK_LIBRARIES(
                                schedulerTest
                                PUBLIC os util common catalog transport gtest qcom ${TAOS_LIB_STATIC} planner scheduler
                        )
                else()                           # }{
                        TARGET_LINK_LIBRARIES(
                                schedulerTest
                                PUBLIC os util common catalog transport qcom ${TAOS_LIB_STATIC} planner scheduler
                        )
                        DEP_ext_gtest(schedulerTest)
                        DEP_ext_stub(schedulerTest)
                        target_include_directories(schedulerTest
                                PUBLIC ${ext_stub_stub} ${ext_stub_addr_any}
                        )
                endif()                          # }
        ENDIF()

        TARGET_INCLUDE_DIRECTORIES(
                schedulerTest
                PUBLIC "${TD_SOURCE_DIR}/include/libs/scheduler/"
                PRIVATE "${TD_SOURCE_DIR}/source/libs/scheduler/inc"
        )
        add_test(
                NAME schedulerTest
                COMMAND schedulerTest
            )
        
ENDIF()