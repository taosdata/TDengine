PROJECT(TDengine)

aux_source_directory(./lib SRC)
aux_source_directory(./lib/arch/avx SRC)
aux_source_directory(./lib/arch/sse42 SRC)
list(REMOVE_ITEM SRC ./lib/lib_openmp.c)

include_directories(${PRJ_HEADER_PATH} ./include ./lib)

add_definitions(-DHAVE_STDINT_H)
add_library(base64 ${SRC} ./lib/arch/ssse3/codec.c ./lib/arch/sse41/codec.c ./lib/arch/avx2/codec.c ./lib/arch/generic/codec.c ./lib/arch/neon32/codec.c ./lib/arch/neon64/codec.c)

target_link_libraries(base64)
