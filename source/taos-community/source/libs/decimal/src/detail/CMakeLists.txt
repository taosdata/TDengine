MESSAGE(STATUS "Building decimal/src/detail")
aux_source_directory(. WIDE_INTEGER_SRC)

SET(CMAKE_CXX_STANDARD 14)
add_library(wideInteger STATIC ${WIDE_INTEGER_SRC})

# When building with MSVC ASan (/fsanitize=address), suppress STL container
# annotations to avoid LNK2038 mismatch with pre-built libs (e.g. rocksdb)
# that were compiled without /fsanitize=address.
if(TD_WINDOWS)
    target_compile_definitions(wideInteger PRIVATE
        _DISABLE_VECTOR_ANNOTATION=1
        _DISABLE_STRING_ANNOTATION=1
    )
endif()

target_include_directories(
    wideInteger
    PUBLIC "${TD_SOURCE_DIR}/source/libs/decimal/inc/"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/intx/"
)
target_link_libraries(
    wideInteger
    PUBLIC
)
