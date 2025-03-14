PROJECT(TDengine)

AUX_SOURCE_DIRECTORY(./src SRC)
include_directories(${PRJ_HEADER_PATH} ./src)

add_definitions(-DHAVE_STDINT_H)
ADD_LIBRARY(jansson ${SRC})

target_link_libraries(jansson)
