cmake_minimum_required(VERSION 3.10)

project(actions_insert LANGUAGES CXX)

# 设置 C++ 标准
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 启用测试支持
enable_testing()

# 添加调试符号信息
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g")

# 添加第三方库


# 添加主库
add_library(actions_insert STATIC 
    src/InsertDataAction.cpp
)

# 包含头文件目录
target_include_directories(actions_insert PUBLIC 
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../utils/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../workflow/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../base
    ${CMAKE_CURRENT_SOURCE_DIR}/../../config/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../components/connector/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../components/formatter/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../components/generator/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../components/reader/csv/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/src/generator/inc/
    ${CMAKE_CURRENT_SOURCE_DIR}/src/pipeline/inc/
    ${CMAKE_CURRENT_SOURCE_DIR}/src/writer/inc/
)

# 链接依赖库
link_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../../utils/build)
link_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../../workflow/build)
link_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../config/build)
link_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../components/connector/build)
link_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../components/formatter/build)
link_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../components/generator/build)
link_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../components/reader/build)
link_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../components/reader/csv/build)
link_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/generator/build)
link_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/pipeline/build)
target_link_libraries(actions_insert PUBLIC
    insert_generator
    insert_pipeline
    actions_config
    components_connector
    components_formatter
    components_generator
    components_reader
    reader_csv
    utils
    workflow
)


# 添加测试
#Test InsertDataAction
add_executable(TestInsertDataAction
    test/TestInsertDataAction.cpp
)
target_link_libraries(TestInsertDataAction PRIVATE 
    actions_insert
    taos
)
add_test(NAME TestInsertDataAction COMMAND TestInsertDataAction)

