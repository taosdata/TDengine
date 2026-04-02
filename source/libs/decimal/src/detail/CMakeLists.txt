MESSAGE(STATUS "Building decimal/src/detail")
aux_source_directory(. WIDE_INTEGER_SRC)

SET(CMAKE_CXX_STANDARD 14)
add_library(wideInteger STATIC ${WIDE_INTEGER_SRC})

target_include_directories(
    wideInteger
    PUBLIC "${TD_SOURCE_DIR}/source/libs/decimal/inc/"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/intx/"
)
target_link_libraries(
    wideInteger
    PUBLIC
)
