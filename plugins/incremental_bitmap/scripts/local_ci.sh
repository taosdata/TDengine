#!/bin/bash

# 本地CI脚本 - 增量位图插件
# 用于在本地运行CI检查，确保代码质量

set -e  # 遇到错误立即退出

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 配置
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="$PROJECT_DIR/build"
SRC_DIR="$PROJECT_DIR/src"
INCLUDE_DIR="$PROJECT_DIR/include"
TEST_DIR="$PROJECT_DIR/test"
DOCS_DIR="$PROJECT_DIR/docs"

# 检查工具函数
check_command() {
    if ! command -v "$1" &> /dev/null; then
        echo -e "${RED}错误: 命令 '$1' 未找到${NC}"
        echo "请安装 $1 后重试"
        exit 1
    fi
}

# 打印状态
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查必要的工具
check_tools() {
    print_status "检查必要的工具..."
    
    local tools=("cmake" "make" "gcc" "valgrind" "clang-format")
    local missing_tools=()
    
    for tool in "${tools[@]}"; do
        if ! command -v "$tool" &> /dev/null; then
            missing_tools+=("$tool")
        fi
    done
    
    if [ ${#missing_tools[@]} -gt 0 ]; then
        print_warning "以下工具未安装: ${missing_tools[*]}"
        echo "建议安装这些工具以获得完整的CI检查"
    else
        print_success "所有必要工具都已安装"
    fi
}

# 代码格式检查
check_code_format() {
    print_status "检查代码格式..."
    
    if ! command -v clang-format &> /dev/null; then
        print_warning "clang-format 未安装，跳过代码格式检查"
        return 0
    fi
    
    local format_issues=0
    
    # 检查C文件格式
    while IFS= read -r -d '' file; do
        if ! clang-format --dry-run --Werror "$file" &> /dev/null; then
            print_error "代码格式问题: $file"
            format_issues=$((format_issues + 1))
        fi
    done < <(find "$SRC_DIR" "$INCLUDE_DIR" -name "*.c" -o -name "*.h" -print0)
    
    if [ $format_issues -eq 0 ]; then
        print_success "代码格式检查通过"
    else
        print_error "发现 $format_issues 个代码格式问题"
        return 1
    fi
}

# 静态代码分析
run_static_analysis() {
    print_status "运行静态代码分析..."
    
    if ! command -v cppcheck &> /dev/null; then
        print_warning "cppcheck 未安装，跳过静态分析"
        return 0
    fi
    
    # 创建输出目录
    mkdir -p "$BUILD_DIR/static_analysis"
    
    # 运行cppcheck
    print_status "运行 cppcheck..."
    cppcheck --enable=all --std=c99 --language=c \
             --suppress=missingIncludeSystem \
             --suppress=unusedFunction \
             --xml --xml-version=2 \
             --output-file="$BUILD_DIR/static_analysis/cppcheck-report.xml" \
             "$SRC_DIR" "$INCLUDE_DIR" 2>&1 || true
    
    # 显示简要报告
    if [ -f "$BUILD_DIR/static_analysis/cppcheck-report.xml" ]; then
        local issues=$(grep -c "<error" "$BUILD_DIR/static_analysis/cppcheck-report.xml" || echo "0")
        if [ "$issues" -gt 0 ]; then
            print_warning "cppcheck 发现 $issues 个问题"
        else
            print_success "cppcheck 未发现问题"
        fi
    fi
    
    print_success "静态分析完成"
}

# 构建项目
build_project() {
    print_status "构建项目..."
    
    # 创建构建目录
    mkdir -p "$BUILD_DIR"
    cd "$BUILD_DIR"
    
    # 配置构建
    print_status "配置CMake..."
    cmake .. \
        -DCMAKE_BUILD_TYPE=Debug \
        -DENABLE_TESTS=ON \
        -DENABLE_COVERAGE=ON \
        -DENABLE_SANITIZERS=ON
    
    # 构建项目
    print_status "编译项目..."
    make -j$(nproc) VERBOSE=1
    
    print_success "项目构建完成"
}

# 运行测试
run_tests() {
    print_status "运行测试..."
    
    cd "$BUILD_DIR"
    
    local test_count=0
    local passed_count=0
    local failed_count=0
    
    # 查找所有测试可执行文件
    for test in test_*; do
        if [ -x "$test" ]; then
            test_count=$((test_count + 1))
            print_status "运行测试: $test"
            
            if ./"$test"; then
                print_success "测试通过: $test"
                passed_count=$((passed_count + 1))
            else
                print_error "测试失败: $test"
                failed_count=$((failed_count + 1))
            fi
        fi
    done
    
    echo
    print_status "测试结果汇总:"
    echo "  总测试数: $test_count"
    echo "  通过: $passed_count"
    echo "  失败: $failed_count"
    
    if [ $failed_count -eq 0 ]; then
        print_success "所有测试通过"
    else
        print_error "$failed_count 个测试失败"
        return 1
    fi
}

# 内存检查
run_memory_check() {
    print_status "运行内存检查..."
    
    if ! command -v valgrind &> /dev/null; then
        print_warning "valgrind 未安装，跳过内存检查"
        return 0
    fi
    
    cd "$BUILD_DIR"
    
    local memory_issues=0
    
    # 对关键测试运行内存检查
    local critical_tests=("test_bitmap_engine_core" "test_observability_comprehensive")
    
    for test in "${critical_tests[@]}"; do
        if [ -x "$test" ]; then
            print_status "使用Valgrind检查: $test"
            
            if valgrind --tool=memcheck --leak-check=full --show-leak-kinds=all \
                       --error-exitcode=1 --track-origins=yes \
                       --log-file="valgrind_${test}.log" \
                       ./"$test" 2>&1; then
                print_success "内存检查通过: $test"
            else
                print_error "内存检查发现问题: $test"
                memory_issues=$((memory_issues + 1))
                
                # 显示内存问题摘要
                if [ -f "valgrind_${test}.log" ]; then
                    echo "内存问题摘要:"
                    grep -E "(ERROR|WARNING)" "valgrind_${test}.log" | head -10
                fi
            fi
        fi
    done
    
    if [ $memory_issues -eq 0 ]; then
        print_success "内存检查完成，未发现问题"
    else
        print_error "发现 $memory_issues 个内存问题"
        return 1
    fi
}

# 代码覆盖率检查
check_coverage() {
    print_status "检查代码覆盖率..."
    
    if ! command -v lcov &> /dev/null; then
        print_warning "lcov 未安装，跳过覆盖率检查"
        return 0
    fi
    
    cd "$BUILD_DIR"
    
    # 生成覆盖率报告
    print_status "生成覆盖率报告..."
    lcov --capture --directory . --output-file coverage.info
    lcov --remove coverage.info '/usr/*' --output-file coverage.info
    lcov --remove coverage.info '*/test/*' --output-file coverage.info
    
    # 显示覆盖率摘要
    local coverage_summary=$(lcov --summary coverage.info)
    echo "$coverage_summary"
    
    # 提取覆盖率百分比
    local coverage_percent=$(echo "$coverage_summary" | grep "lines" | grep -o "[0-9.]*%" | head -1)
    
    if [ -n "$coverage_percent" ]; then
        print_status "代码覆盖率: $coverage_percent"
        
        # 检查覆盖率是否达到要求（例如80%）
        local coverage_number=$(echo "$coverage_percent" | sed 's/%//')
        if (( $(echo "$coverage_number >= 80" | bc -l) )); then
            print_success "代码覆盖率达标"
        else
            print_warning "代码覆盖率较低: $coverage_percent"
        fi
    fi
    
    print_success "覆盖率检查完成"
}

# 文档检查
check_documentation() {
    print_status "检查文档..."
    
    local missing_docs=()
    local required_docs=(
        "README.md"
        "docs/installation_guide.md"
        "docs/api_usage_guide.md"
        "docs/troubleshooting_guide.md"
        "docs/observability_metrics.md"
    )
    
    # 检查必要文档是否存在
    for doc in "${required_docs[@]}"; do
        if [ ! -f "$PROJECT_DIR/$doc" ]; then
            missing_docs+=("$doc")
        fi
    done
    
    if [ ${#missing_docs[@]} -gt 0 ]; then
        print_error "缺少必要文档: ${missing_docs[*]}"
        return 1
    fi
    
    # 检查文档链接
    print_status "检查文档链接..."
    local broken_links=0
    
    while IFS= read -r -d '' file; do
        if [ -f "$file" ]; then
            while IFS= read -r link; do
                target=$(echo "$link" | sed 's/\[.*\](\([^)]*\))/\1/')
                if [[ "$target" != http* && "$target" != \#* ]]; then
                    if [ ! -f "$target" ] && [ ! -f "$(dirname "$file")/$target" ]; then
                        print_warning "断开的链接: $target in $file"
                        broken_links=$((broken_links + 1))
                    fi
                fi
            done < <(grep -o "\[.*\]([^)]*)" "$file" || true)
        fi
    done < <(find "$DOCS_DIR" -name "*.md" -print0)
    
    if [ $broken_links -eq 0 ]; then
        print_success "文档链接检查通过"
    else
        print_warning "发现 $broken_links 个断开的链接"
    fi
    
    print_success "文档检查完成"
}

# 性能基准测试
run_performance_benchmark() {
    print_status "运行性能基准测试..."
    
    cd "$BUILD_DIR"
    
    # 检查是否有性能测试
    if [ -x "test_performance" ]; then
        print_status "运行性能测试..."
        ./test_performance
    else
        print_status "没有找到性能测试，运行基本性能检查..."
        
        # 对主要测试进行时间测量
        local main_tests=("test_bitmap_engine_core" "test_observability_comprehensive")
        
        for test in "${main_tests[@]}"; do
            if [ -x "$test" ]; then
                print_status "测量 $test 执行时间..."
                time ./"$test" > /dev/null 2>&1
            fi
        done
    fi
    
    print_success "性能基准测试完成"
}

# 生成报告
generate_report() {
    print_status "生成CI报告..."
    
    local report_file="$BUILD_DIR/ci_report.txt"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    cat > "$report_file" << EOF
增量位图插件 - CI检查报告
生成时间: $timestamp

=== 检查项目 ===
1. 工具检查: ${TOOLS_CHECK_RESULT:-未执行}
2. 代码格式: ${FORMAT_CHECK_RESULT:-未执行}
3. 静态分析: ${STATIC_ANALYSIS_RESULT:-未执行}
4. 项目构建: ${BUILD_RESULT:-未执行}
5. 测试执行: ${TEST_RESULT:-未执行}
6. 内存检查: ${MEMORY_CHECK_RESULT:-未执行}
7. 代码覆盖率: ${COVERAGE_RESULT:-未执行}
8. 文档检查: ${DOC_CHECK_RESULT:-未执行}
9. 性能测试: ${PERFORMANCE_RESULT:-未执行}

=== 详细结果 ===
$CI_DETAILS

=== 总结 ===
总检查项目: 9
通过: $PASSED_COUNT
失败: $FAILED_COUNT
警告: $WARNING_COUNT

EOF

    print_success "CI报告已生成: $report_file"
    cat "$report_file"
}

# 主函数
main() {
    echo "=========================================="
    echo "    增量位图插件 - 本地CI检查"
    echo "=========================================="
    echo
    
    # 初始化计数器
    PASSED_COUNT=0
    FAILED_COUNT=0
    WARNING_COUNT=0
    CI_DETAILS=""
    
    # 检查工具
    if check_tools; then
        TOOLS_CHECK_RESULT="通过"
        PASSED_COUNT=$((PASSED_COUNT + 1))
    else
        TOOLS_CHECK_RESULT="失败"
        FAILED_COUNT=$((FAILED_COUNT + 1))
    fi
    
    # 代码格式检查
    if check_code_format; then
        FORMAT_CHECK_RESULT="通过"
        PASSED_COUNT=$((PASSED_COUNT + 1))
    else
        FORMAT_CHECK_RESULT="失败"
        FAILED_COUNT=$((FAILED_COUNT + 1))
    fi
    
    # 静态分析
    if run_static_analysis; then
        STATIC_ANALYSIS_RESULT="通过"
        PASSED_COUNT=$((PASSED_COUNT + 1))
    else
        STATIC_ANALYSIS_RESULT="失败"
        FAILED_COUNT=$((FAILED_COUNT + 1))
    fi
    
    # 构建项目
    if build_project; then
        BUILD_RESULT="通过"
        PASSED_COUNT=$((PASSED_COUNT + 1))
    else
        BUILD_RESULT="失败"
        FAILED_COUNT=$((FAILED_COUNT + 1))
    fi
    
    # 运行测试
    if run_tests; then
        TEST_RESULT="通过"
        PASSED_COUNT=$((PASSED_COUNT + 1))
    else
        TEST_RESULT="失败"
        FAILED_COUNT=$((FAILED_COUNT + 1))
    fi
    
    # 内存检查
    if run_memory_check; then
        MEMORY_CHECK_RESULT="通过"
        PASSED_COUNT=$((PASSED_COUNT + 1))
    else
        MEMORY_CHECK_RESULT="失败"
        FAILED_COUNT=$((FAILED_COUNT + 1))
    fi
    
    # 代码覆盖率
    if check_coverage; then
        COVERAGE_RESULT="通过"
        PASSED_COUNT=$((PASSED_COUNT + 1))
    else
        COVERAGE_RESULT="失败"
        FAILED_COUNT=$((FAILED_COUNT + 1))
    fi
    
    # 文档检查
    if check_documentation; then
        DOC_CHECK_RESULT="通过"
        PASSED_COUNT=$((PASSED_COUNT + 1))
    else
        DOC_CHECK_RESULT="失败"
        FAILED_COUNT=$((FAILED_COUNT + 1))
    fi
    
    # 性能测试
    if run_performance_benchmark; then
        PERFORMANCE_RESULT="通过"
        PASSED_COUNT=$((PASSED_COUNT + 1))
    else
        PERFORMANCE_RESULT="失败"
        FAILED_COUNT=$((FAILED_COUNT + 1))
    fi
    
    echo
    echo "=========================================="
    echo "                CI检查完成"
    echo "=========================================="
    
    # 生成报告
    generate_report
    
    # 返回结果
    if [ $FAILED_COUNT -eq 0 ]; then
        print_success "所有检查通过！"
        exit 0
    else
        print_error "$FAILED_COUNT 个检查失败"
        exit 1
    fi
}

# 处理命令行参数
case "${1:-}" in
    --help|-h)
        echo "用法: $0 [选项]"
        echo "选项:"
        echo "  --help, -h     显示帮助信息"
        echo "  --format       仅检查代码格式"
        echo "  --static       仅运行静态分析"
        echo "  --build        仅构建项目"
        echo "  --test         仅运行测试"
        echo "  --memory       仅运行内存检查"
        echo "  --coverage     仅检查代码覆盖率"
        echo "  --docs         仅检查文档"
        echo "  --performance  仅运行性能测试"
        echo "  --quick        快速检查（跳过耗时项目）"
        ;;
    --format)
        check_code_format
        ;;
    --static)
        run_static_analysis
        ;;
    --build)
        build_project
        ;;
    --test)
        build_project
        run_tests
        ;;
    --memory)
        build_project
        run_memory_check
        ;;
    --coverage)
        build_project
        check_coverage
        ;;
    --docs)
        check_documentation
        ;;
    --performance)
        build_project
        run_performance_benchmark
        ;;
    --quick)
        # 快速检查：跳过耗时项目
        check_tools
        check_code_format
        check_documentation
        ;;
    "")
        # 默认运行所有检查
        main
        ;;
    *)
        print_error "未知选项: $1"
        echo "使用 --help 查看帮助信息"
        exit 1
        ;;
esac
