PROJECT(TDengine)

AUX_SOURCE_DIRECTORY(./src SRC)
list(REMOVE_ITEM SRC ./src/jwt-gnutls.c)

include_directories(${PRJ_HEADER_PATH} ../inc)

ADD_LIBRARY(jwt ${SRC})
target_link_libraries(jwt jansson ssl crypto z)
