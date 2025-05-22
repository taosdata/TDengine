cmake_minimum_required(VERSION 3.10)

project(actions LANGUAGES CXX)

# 设置 C++ 标准
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 启用测试支持
enable_testing()

# 添加调试符号信息
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g")

# 添加子目录
add_subdirectory(config)

# 添加 base 目录
add_library(actions_base INTERFACE)
target_include_directories(actions_base INTERFACE base)

# 添加 create 目录
add_library(actions_create STATIC
    create/CreateChildTableAction.cpp
    create/CreateDatabaseAction.cpp
    create/CreateSuperTableAction.cpp
)
target_include_directories(actions_create PUBLIC create)
target_link_libraries(actions_create PUBLIC actions_config actions_base)

# 添加 insert 目录
add_library(actions_insert STATIC
    insert/InsertDataAction.cpp
)
target_include_directories(actions_insert PUBLIC insert)
target_link_libraries(actions_insert PUBLIC actions_config actions_base)

# 添加 query 目录
add_library(actions_query STATIC
    query/QueryDataAction.cpp
)
target_include_directories(actions_query PUBLIC query)
target_link_libraries(actions_query PUBLIC actions_config actions_base)

# 添加 subscribe 目录
add_library(actions_subscribe STATIC
    subscribe/SubscribeDataAction.cpp
)
target_include_directories(actions_subscribe PUBLIC subscribe)
target_link_libraries(actions_subscribe PUBLIC actions_config actions_base)

