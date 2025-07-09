cmake_minimum_required(VERSION 3.16)

project(addr2line)

add_executable(addr2line addr2line.c)
target_include_directories(addr2line PRIVATE ${DWARF_BASE_DIR}/include/libdwarf)
target_link_directories(addr2line PRIVATE ${DWARF_BASE_DIR}/lib)
target_include_directories(addr2line PRIVATE ${ZLIB_BASE_DIR}/include)
target_link_directories(addr2line PRIVATE ${ZLIB_BASE_DIR}/lib)
target_link_libraries(addr2line PRIVATE libdwarf.a z)
install(TARGETS addr2line)

