cmake_minimum_required(VERSION 3.17)
project(FilterPicker C)

set(CMAKE_C_STANDARD 11)

include_directories(.)

add_executable(FilterPicker
        ew_bridge.h
        FilterPicker5.c
        FilterPicker5.h
        FilterPicker5_Memory.c
        FilterPicker5_Memory.h
        PickData.c
        PickData.h
        picker_func_test_memory.c
        sachdr.h)

target_link_libraries(FilterPicker m)