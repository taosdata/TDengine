
MESSAGE(STATUS "build parser unit test")

IF(NOT TD_DARWIN)
    # GoogleTest requires at least C++11
    if(${FALLBACK})          # {
        SET(CMAKE_CXX_STANDARD 11)
    else()                   # }{
        SET(CMAKE_CXX_STANDARD 14)
    endif()                  # }
    AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

    ADD_EXECUTABLE(parserTest ${SOURCE_LIST})

    TARGET_INCLUDE_DIRECTORIES(
            parserTest
            PUBLIC "${TD_SOURCE_DIR}/include/libs/parser/"
            PRIVATE "${TD_SOURCE_DIR}/source/libs/parser/inc"
    )

    if(${FALLBACK})                            # {
        TARGET_LINK_LIBRARIES(
                parserTest
                PUBLIC os util common nodes parser catalog transport gtest function planner qcom
        )
    else()                                     # }{
        TARGET_LINK_LIBRARIES(
                parserTest
                PUBLIC os util common nodes parser catalog transport function planner qcom
        )
        DEP_ext_gtest(parserTest)
        if(${TD_WINDOWS})           # {
                DEP_ext_wingetopt(parserTest)
        endif()                     # }
        DEP_ext_stub(parserTest)
        target_include_directories(parserTest
                PUBLIC
                ${ext_stub_stub} ${ext_stub_addr_any}
        )
    endif()                                    # }

    if(${BUILD_WINGETOPT})
        if(${FALLBACK})         # {
            target_include_directories(
                parserTest
                PUBLIC "${TD_SOURCE_DIR}/contrib/wingetopt/src"
            )
            target_link_libraries(parserTest PUBLIC wingetopt)
        endif()                 # }
    endif()

    add_test(
        NAME parserTest
        COMMAND parserTest
    )
ENDIF()
