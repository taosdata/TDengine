cmake_minimum_required(VERSION 3.18)

project(perfcol)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra")

set(PERFCOL_SOURCE_FILES
    src/perfCollector.c
)

set(PERFCOL_HEADER_FILES
    inc/perfCollector.h
)

add_library(${PROJECT_NAME} STATIC ${PERFCOL_SOURCE_FILES} ${PERFCOL_HEADER_FILES})

target_include_directories(${PROJECT_NAME} PUBLIC 
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(${PROJECT_NAME} 
    pthread
    m
)

set_target_properties(${PROJECT_NAME} PROPERTIES
    C_STANDARD 99
    POSITION_INDEPENDENT_CODE ON
)

if(NOT TDENGINE_COMMUNITY_DIR)
    target_compile_options(${PROJECT_NAME} PRIVATE -DTDENGINE)
endif()

# Install library and headers
install(TARGETS ${PROJECT_NAME} DESTINATION lib)
install(FILES ${PERFCOL_HEADER_FILES} DESTINATION include/libs/perfcol)