cmake_minimum_required(VERSION 3.10)

project(actions_create LANGUAGES CXX)

# 设置 C++ 标准
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 启用测试支持
enable_testing()

# 添加调试符号信息
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g")

# 添加第三方库


# 添加主库
add_library(actions_create STATIC 
    src/CreateDatabaseAction.cpp
    src/CreateSuperTableAction.cpp
    src/CreateChildTableAction.cpp
)

# 包含头文件目录
target_include_directories(actions_create PUBLIC 
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../utils/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../workflow/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../base
    ${CMAKE_CURRENT_SOURCE_DIR}/../../config/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../components/connector/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../components/formatter/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../components/generator/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../components/reader/csv/inc
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
target_link_libraries(actions_create PUBLIC
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
# test CreateDatabaseAction
add_executable(TestCreateDatabaseAction
    test/TestCreateDatabaseAction.cpp
)
target_link_libraries(TestCreateDatabaseAction PRIVATE 
    actions_create
    taos
)
add_test(NAME TestCreateDatabaseAction COMMAND TestCreateDatabaseAction)

# test CreateSuperTableAction
add_executable(TestCreateSuperTableAction
    test/TestCreateSuperTableAction.cpp
)
target_link_libraries(TestCreateSuperTableAction PRIVATE 
    actions_create
    taos
)
add_test(NAME TestCreateSuperTableAction COMMAND TestCreateSuperTableAction)

# test CreateChildTableAction
add_executable(TestCreateChildTableAction
    test/TestCreateChildTableAction.cpp
)
link_directories(${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(TestCreateChildTableAction PRIVATE
    actions_create
    taos
)
add_test(NAME TestCreateChildTableAction COMMAND TestCreateChildTableAction)

