
MESSAGE(STATUS "build parser unit test")

IF(NOT TD_DARWIN)
    # GoogleTest requires at least C++11
    SET(CMAKE_CXX_STANDARD 11)
    AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

    ADD_EXECUTABLE(parserTest ${SOURCE_LIST})

    TARGET_INCLUDE_DIRECTORIES(
            parserTest
            PUBLIC "${TD_SOURCE_DIR}/include/libs/parser/"
            PRIVATE "${TD_SOURCE_DIR}/source/libs/parser/inc"
    )

    TARGET_LINK_LIBRARIES(
            parserTest
            PUBLIC os util common nodes parser catalog transport gtest function planner qcom
    )

    if(${BUILD_WINGETOPT})
        target_include_directories(
            parserTest
            PUBLIC "${TD_SOURCE_DIR}/contrib/wingetopt/src"
        )
        target_link_libraries(parserTest PUBLIC wingetopt)
    endif()

    add_test(
        NAME parserTest
        COMMAND parserTest
    )
ENDIF()