PROJECT(TDengine)

aux_source_directory(./src SRC)
list(REMOVE_ITEM SRC ./src/jwt-gnutls.c)

include_directories(${PRJ_HEADER_PATH} ../inc)

add_library(jwt ${SRC})
target_link_libraries(jwt jansson ssl crypto z)
