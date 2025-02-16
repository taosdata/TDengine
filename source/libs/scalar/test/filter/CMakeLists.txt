
MESSAGE(STATUS "build filter unit test")

IF(TD_DARWIN)
        # GoogleTest requires at least C++11
        if(${FALLBACK})          # {
            SET(CMAKE_CXX_STANDARD 11)
            AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

            ADD_EXECUTABLE(filterTest ${SOURCE_LIST})
            TARGET_LINK_LIBRARIES(
                    filterTest
                    PUBLIC os util common gtest qcom function nodes scalar parser catalog transport
            )

            TARGET_INCLUDE_DIRECTORIES(
                    filterTest
                    PUBLIC "${TD_SOURCE_DIR}/include/libs/scalar/"
                    PRIVATE "${TD_SOURCE_DIR}/source/libs/scalar/inc"
            )
        else()                   # }{
            SET(CMAKE_CXX_STANDARD 14)
            AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

            ADD_EXECUTABLE(filterTest ${SOURCE_LIST})
            TARGET_LINK_LIBRARIES(
                    filterTest
                    PUBLIC os util common qcom function nodes scalar parser catalog transport
            )
            DEP_ext_gtest(filterTest)
            DEP_ext_stub(filterTest)

            TARGET_INCLUDE_DIRECTORIES(
                    filterTest
                    PUBLIC "${TD_SOURCE_DIR}/include/libs/scalar/"
                    PRIVATE "${TD_SOURCE_DIR}/source/libs/scalar/inc"
                    PRIVATE ${ext_stub_stub} ${ext_stub_addr_any}
            )
        endif()                  # }
ENDIF()
