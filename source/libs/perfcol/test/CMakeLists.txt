cmake_minimum_required(VERSION 3.18)

project(perfcol_demo)

set(CMAKE_C_STANDARD 99)

set(TEST_SOURCE_FILES
    perfCollectorTest.c
)

add_executable(${PROJECT_NAME} ${TEST_SOURCE_FILES})

target_include_directories(${PROJECT_NAME} PRIVATE 
    ${CMAKE_CURRENT_SOURCE_DIR}/../inc
)

target_link_libraries(${PROJECT_NAME} 
    perfcol
    pthread
    m
)

if(NOT TDENGINE_COMMUNITY_DIR)
    target_compile_options(${PROJECT_NAME} PRIVATE -DTDENGINE)
endif()