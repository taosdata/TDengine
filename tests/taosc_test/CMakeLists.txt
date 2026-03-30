# CMAKE_MINIMUM_REQUIRED(VERSION 2.8...3.20)
# PROJECT(TDengine)

aux_source_directory(src OS_SRC)

# taoscTest
add_executable(taoscTest "taoscTest.cpp")
DEP_ext_gtest(taoscTest)
target_link_libraries(taoscTest PRIVATE ${TAOS_NATIVE_LIB} util)
target_include_directories(
    taoscTest
    PUBLIC "${TD_SOURCE_DIR}/include/os"
)

if(${TD_LINUX})
    add_test(
        NAME taoscTest
        COMMAND taoscTest
    )
endif(${TD_LINUX})

