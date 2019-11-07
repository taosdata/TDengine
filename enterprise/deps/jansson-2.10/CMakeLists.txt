PROJECT(TDengine)

aux_source_directory(./src SRC)
include_directories(${PRJ_HEADER_PATH} ./src)

add_definitions(-DHAVE_STDINT_H)
add_library(jansson ${SRC} )

target_link_libraries(jansson)
