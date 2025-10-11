cmake_minimum_required(VERSION 3.16)
project(incremental_bitmap_plugin)

set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 17)

# 添加编译标志来抑制可执行栈警告
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wl,-z,noexecstack")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wl,-z,noexecstack")

# 选项: 强制使用 mock 引擎
option(USE_MOCK "Force using mock storage engine and disable TMQ" OFF)
if(USE_MOCK)
    add_compile_definitions(USE_MOCK=1)
endif()

# 选项: 构建真实TDengine全链路E2E测试（默认关闭，不会自动运行）
option(E2E_TDENGINE_REAL_TESTS "Build real TDengine end-to-end tests" OFF)

# 选项: 构建taosX插件接口（默认关闭）
option(BUILD_TAOSX_PLUGIN "Build taosX plugin interface" OFF)


# 设置TDengine路径
set(TDENGINE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..)
set(TDENGINE_BUILD_DIR ${TDENGINE_ROOT}/build)

# 包含目录
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${TDENGINE_ROOT}/include
    ${TDENGINE_BUILD_DIR}/include
    ${TDENGINE_ROOT}/deps/roaring/include
)

# 设置库目录
link_directories(
    ${TDENGINE_BUILD_DIR}/lib
    ${TDENGINE_BUILD_DIR}/build/lib
)

# 源文件
set(SOURCES
    src/bitmap_engine.c
    src/event_interceptor.c
    src/backup_coordinator.c
    src/skiplist.c
    src/simple_bitmap.c
    src/roaring_bitmap.c
    src/ring_buffer.c
    src/storage_engine_interface.c
    src/tdengine_storage_engine.c
    src/incremental_backup_tool.c
    src/observability.c
)

# 创建动态库
add_library(incremental_bitmap_plugin SHARED ${SOURCES})

# 链接库
if(NOT USE_MOCK)
    target_link_libraries(incremental_bitmap_plugin taos pthread roaring)
else()
    message(WARNING "Building with USE_MOCK=ON: TDengine client not linked; TMQ backend disabled")
    target_link_libraries(incremental_bitmap_plugin pthread roaring)
endif()

# 设置输出目录
set_target_properties(incremental_bitmap_plugin PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/build
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/build
)

# taosX插件接口（可选）
if(BUILD_TAOSX_PLUGIN)
    add_subdirectory(taosx_plugin)
endif()

# 安装规则
install(TARGETS incremental_bitmap_plugin
    LIBRARY DESTINATION /usr/local/taos/plugins/backup
)

# 测试程序
if(BUILD_TESTING)
    # 环形队列测试
    add_executable(test_ring_buffer test/test_ring_buffer.c src/ring_buffer.c)
    target_link_libraries(test_ring_buffer pthread)
    
    # 位图引擎核心功能测试
    add_executable(test_bitmap_engine_core test/test_bitmap_engine_core.c src/bitmap_engine.c src/skiplist.c src/simple_bitmap.c src/roaring_bitmap.c)
    target_link_libraries(test_bitmap_engine_core pthread roaring)
    
    # 抽象层接口测试
    add_executable(test_abstraction_layer test/test_abstraction_layer.c src/simple_bitmap.c src/roaring_bitmap.c)
    target_link_libraries(test_abstraction_layer pthread roaring)
    
    # RoaringBitmap专用测试
    add_executable(test_roaring_bitmap_specific test/test_roaring_bitmap_specific.c src/simple_bitmap.c src/roaring_bitmap.c)
    target_link_libraries(test_roaring_bitmap_specific pthread roaring)
    
    # 事件拦截器测试
    add_executable(test_event_interceptor 
        test/test_event_interceptor.c 
        src/event_interceptor.c
        src/bitmap_engine.c
        src/ring_buffer.c
        src/skiplist.c
        src/simple_bitmap.c
        src/roaring_bitmap.c
    )
    target_link_libraries(test_event_interceptor pthread roaring)
    
    # 备份协同器测试
    add_executable(test_backup_coordinator 
        test/test_backup_coordinator.c 
        src/backup_coordinator.c
        src/event_interceptor.c
        src/bitmap_engine.c
        src/ring_buffer.c
        src/skiplist.c
        src/simple_bitmap.c
        src/roaring_bitmap.c
    )
    target_link_libraries(test_backup_coordinator pthread roaring)
    
    # 状态转换测试
    add_executable(test_state_transitions 
        test/test_state_transitions.c 
        src/bitmap_engine.c
        src/skiplist.c
        src/simple_bitmap.c
        src/roaring_bitmap.c
    )
    target_link_libraries(test_state_transitions pthread roaring)
    
    # 跳表测试
    add_executable(test_skiplist 
        test/test_skiplist.c 
        src/skiplist.c
    )
    target_link_libraries(test_skiplist pthread)
    
    # 可观测性接口测试
    add_executable(test_observability_interface test/test_observability_interface.c src/observability.c)
    target_link_libraries(test_observability_interface)
    
    # 增强可观测性指标测试
    add_executable(test_observability_enhanced 
        test/test_observability_enhanced.c 
        src/observability.c
        src/bitmap_engine.c
        src/event_interceptor.c
        src/ring_buffer.c
        src/skiplist.c
        src/simple_bitmap.c
        src/roaring_bitmap.c
    )
    target_link_libraries(test_observability_enhanced pthread roaring)
    
                    # 全面可观测性指标测试
                add_executable(test_observability_comprehensive 
                    test/test_observability_comprehensive.c 
                    src/observability.c
                    src/bitmap_engine.c
                    src/event_interceptor.c
                    src/ring_buffer.c
                    src/skiplist.c
                    src/simple_bitmap.c
                    src/roaring_bitmap.c
                )
                target_link_libraries(test_observability_comprehensive pthread roaring)
                
                # Offset语义验证测试
                add_executable(test_offset_semantics 
                    test/test_offset_semantics.c
                )
                target_link_libraries(test_offset_semantics pthread)
                
                # 真实TDengine Offset语义验证测试（仅在显式开启E2E且NOT USE_MOCK时构建）
                if(E2E_TDENGINE_REAL_TESTS AND NOT USE_MOCK)
                    add_executable(test_offset_semantics_realtime 
                        test/test_offset_semantics_realtime.c
                    )
                    target_link_libraries(test_offset_semantics_realtime pthread taos)

                    set(TEST_OFFSET_REAL_SRCS
                        test/test_offset_semantics_real.c
                        src/storage_engine_interface.c
                        src/tdengine_storage_engine.c
                        src/event_interceptor.c
                        src/bitmap_engine.c
                        src/ring_buffer.c
                        src/skiplist.c
                        src/simple_bitmap.c
                        src/roaring_bitmap.c
                    )
                    add_executable(test_offset_semantics_real ${TEST_OFFSET_REAL_SRCS})
                    target_link_libraries(test_offset_semantics_real pthread roaring taos)
                else()
                    message(STATUS "跳过构建真实Offset语义测试: 需要 E2E_TDENGINE_REAL_TESTS=ON 且 NOT USE_MOCK")
                endif()
                
                # 故障注入测试
                add_executable(test_fault_injection 
                    test/test_fault_injection.c
                )
                target_link_libraries(test_fault_injection pthread)
                
                # PITR E2E测试（完整版）
                add_executable(test_pitr_e2e 
                    test/test_pitr_e2e.c
                    test/pitr_e2e_test.c
                    src/bitmap_engine.c
                    src/backup_coordinator.c
                    src/event_interceptor.c
                    src/ring_buffer.c
                    src/skiplist.c
                    src/simple_bitmap.c
                    src/roaring_bitmap.c
                )
                target_link_libraries(test_pitr_e2e pthread roaring)
                
                # PITR E2E测试（简化版）
                add_executable(test_pitr_e2e_simple 
                    test/test_pitr_e2e_simple.c
                    test/pitr_e2e_test.c
                    src/bitmap_engine.c
                    src/backup_coordinator.c
                    src/event_interceptor.c
                    src/ring_buffer.c
                    src/skiplist.c
                    src/simple_bitmap.c
                    src/roaring_bitmap.c
                )
                target_link_libraries(test_pitr_e2e_simple pthread roaring)
                
                # 最小化一致性验证数据生成器
                add_executable(test_consistency_minimal 
                    test/test_consistency_minimal.c
                    test/pitr_e2e_test.c
                    src/bitmap_engine.c
                    src/backup_coordinator.c
                    src/event_interceptor.c
                    src/ring_buffer.c
                    src/skiplist.c
                    src/simple_bitmap.c
                    src/roaring_bitmap.c
                )
                target_link_libraries(test_consistency_minimal pthread roaring)
    
    # 重试机制测试
    # add_executable(test_retry_mechanism 
    #     test/test_retry_mechanism.c 
    #     src/backup_coordinator.c
    #     src/event_interceptor.c
    #     src/bitmap_engine.c
    #     src/ring_buffer.c
    #     src/skiplist.c
    #     src/simple_bitmap.c
    #     src/roaring_bitmap.c
    # )
    # target_link_libraries(test_retry_mechanism pthread roaring)

    # TMQ 集成测试
    set(TEST_TMQ_SRCS
        test/test_tmq_integration.c
        src/storage_engine_interface.c
        test/mock_storage_engine.c
        src/event_interceptor.c
        src/bitmap_engine.c
        src/ring_buffer.c
        src/skiplist.c
        src/simple_bitmap.c
        src/roaring_bitmap.c
    )
    if(NOT USE_MOCK)
        list(APPEND TEST_TMQ_SRCS src/tdengine_storage_engine.c)
    endif()
    add_executable(test_tmq_integration ${TEST_TMQ_SRCS})
    if(NOT USE_MOCK)
        target_link_libraries(test_tmq_integration pthread roaring taos)
    else()
        message(WARNING "Building tests with USE_MOCK=ON: test_tmq_integration will run in mock mode")
        target_link_libraries(test_tmq_integration pthread roaring)
    endif()
    
                    # 设置测试输出目录
                set_target_properties(test_ring_buffer test_bitmap_engine_core test_abstraction_layer test_roaring_bitmap_specific test_event_interceptor test_backup_coordinator test_state_transitions test_skiplist test_observability_interface test_observability_enhanced test_observability_comprehensive test_offset_semantics test_fault_injection test_pitr_e2e test_pitr_e2e_simple test_tmq_integration
                    PROPERTIES
                    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/build
                )
endif()

# 真实 TDengine 全链路 E2E 测试（默认不构建，需显式开启 E2E_TDENGINE_REAL_TESTS）
if(E2E_TDENGINE_REAL_TESTS)
    if(USE_MOCK)
        message(WARNING "E2E_TDENGINE_REAL_TESTS=ON 但 USE_MOCK=ON；将跳过真实E2E测试目标的构建")
    else()
        add_executable(test_e2e_tdengine_real
            test/test_e2e_tdengine_real.c
            test/e2e_consistency.c
            test/e2e_perf.c
            src/bitmap_engine.c
            src/backup_coordinator.c
            src/event_interceptor.c
            src/ring_buffer.c
            src/skiplist.c
            src/simple_bitmap.c
            src/roaring_bitmap.c
            src/storage_engine_interface.c
            src/tdengine_storage_engine.c
        )
        target_link_libraries(test_e2e_tdengine_real pthread roaring taos)
        set_target_properties(test_e2e_tdengine_real PROPERTIES
            RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/build
        )
    endif()
else()
    message(STATUS "E2E_TDENGINE_REAL_TESTS=OFF: 跳过真实TDengine全链路E2E测试目标的构建")
endif()

# taosdump增量对比测试
add_executable(test_taosdump_comparison
    test/test_taosdump_comparison.c
    src/bitmap_engine.c
    src/backup_coordinator.c
    src/event_interceptor.c
    src/ring_buffer.c
    src/skiplist.c
    src/simple_bitmap.c
    src/roaring_bitmap.c
)
target_link_libraries(test_taosdump_comparison pthread roaring taos)
set_target_properties(test_taosdump_comparison PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/build
)

