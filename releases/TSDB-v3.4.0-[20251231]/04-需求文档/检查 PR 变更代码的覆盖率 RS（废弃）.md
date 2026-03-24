# 检查 PR 变更代码的覆盖率 RS（废弃）

## 1. 引言

### 1.1 术语与缩写名词

无

### 1.2 相关文档资料

1. [本地运行覆盖率的方法](https://taosdata.feishu.cn/wiki/BAe8w7y4HiZulGklgFKcOSfPnva)
2. JIRA [TD-37719](https://jira.taosdata.com:18080/browse/TD-37719)

### 1.3 优先级要求

高，越快越好，元旦前一定要出来

### 1.4 版本要求

无

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/08/28 | 1.0 | 关胜亮 | 新建 |

## 3. 需求目标

每次 PR 提交时，检查变更代码的测试覆盖程度

## 4. 功能需求

1. 关于 CI 测试和覆盖率测试
   - 两者合并运行，同时检查结果
   - 两者使用相同的 ci.task 文件、单元测试列表
   - PR 提交时触发
   - PR 结束时生成变更文件的覆盖率结果，作为代码是否可以合并的佐证
   - 覆盖率结果以可读文本方式展现，建议提供快速的本地 html 页面（可下载）
2. 定期运行完整覆盖率程序，本地和 Codecov 都可查看
3. 原有的覆盖率脚本不再维护

## 5. 性能需求

无

## 6. 建议方案

### 6.1 启用覆盖率分析

在编译代码时添加 GCC 的特殊选项，生成覆盖率分析所需的数据文件
1. 编译命令示例​
```makefile {wrap}
gcc -fprofile-arcs -ftest-coverage -o my_program my_program.c
```

1. 作用​
  - `-fprofile-arcs`：生成 `.gcno`文件（记录代码的控制流图信息）
  - `-ftest-coverage`：在目标文件中插入覆盖率统计代码，为后续生成运行时数据做准备

### 6.2 改变覆盖率文件路径

通过环境变量 `GCOV_PREFIX` 和 `GCOV_PREFIX_STRIP`，为每个并发实例指定独立的输出目录
```bash {wrap}
export GCOV_PREFIX="/tmp/gcov_data/$$"  # $$ 为当前进程或者 docker ID
export GCOV_PREFIX_STRIP=3  # 裁剪编译时路径的前 3级目录（按需调整）
./your_program
```

### 6.3 生成覆盖率文件

运行后产生 `.gcda` 文件，记录代码的实际执行路径和次数

### 6.4 合并覆盖率文件

收集各实例的 `.gcda` 文件后，使用 `lcov` 或 `gcovr` 合并覆盖率数据
1. 使用 lcov 合并​
  - 对每个`.gcda` 目录运行 `lcov` 生成中间 `.info` 文件
  ```bash {wrap}
  lcov -c -d /tmp/gcov_data/12345/ -o coverage_12345.info --rc lcov_branch_coverage=1
  ```

  - 合并所有 `.info` 文件​
  ```bash {wrap}
  lcov -a coverage_12345.info -a coverage_67890.info -o merged_coverage.info
  ```

1. 使用 `gcovr` 合并
  - 直接指定多个`.gcda` 目录，自动合并数据
  ```bash {wrap}
  gcovr -r /path/to/source_code \
        --add-tracefile /tmp/gcov_data/12345/ \
        --add-tracefile /tmp/gcov_data/67890/ \
        -o merged_report.html
  ```

### 6.5 获取变更文件列表

在 CI 脚本中通过 Git 命令提取修改的 C/H 文件，
```bash {wrap}

## 7. 获取当前 PR 的源分支和目标分支的差异文件

CHANGED_FILES=$(git diff --name-only --diff-filter=d origin/main...HEAD | grep -E '\.(c|h)$')
```

关键参数​：
- `--diff-filter=d`：排除已删除文件。
- `origin/main...HEAD`：对比目标分支（如 main）与当前 PR 分支的差异

### 7.1 生成覆盖率数据

使用 `lcov` 收集并过滤覆盖率数据：
```bash {wrap}
lcov --capture --directory . --output-file coverage.info
lcov --remove coverage.info '*/tests/*' '*/third_party/*' -o filtered.info
```

### 7.2 关联变更行与覆盖率

提取变更行号，针对每个变更文件，生成变更行号列表
```bash {wrap}
for file in $CHANGED_FILES; do
  git diff -U0 origin/main...HEAD -- $file | grep '^@@' | awk '{print $3}' | cut -d',' -f1 | sed 's/^+//'done > changed_lines.txt
```

### 7.3 匹配覆盖率数据

解析 `filtered.info`，提取变更文件的每行覆盖率
```bash {wrap}
lcov --list filtered.info | grep -E "$(echo $CHANGED_FILES | tr '\n' '|')" > coverage_lines.txt
```

输出示例，`1` 表示覆盖，`0` 表示未覆盖
```bash {wrap}
src/module1/file1.c:42:1
src/module1/file1.c:87:0
src/module1/file1.c:88:0
```

### 7.4 交叉分析未覆盖变更行

对比 `changed_lines.txt` 和 `coverage_lines.txt`，标记未覆盖的变更行
```bash {wrap}
while read -r line; doif grep -q "$line:0" coverage_lines.txt; thenecho "UNCOVERED: $file:$line"fidone < changed_lines.txt
```

### 7.5 检查覆盖率结果

对于覆盖率不足 60% 的 PR，标记失败

### 7.6 集成到 CI 报告

使用 `genhtml`或者其他方法生成 HTML 报告，并高亮变更文件和未覆盖的变更行。
```bash {wrap}
genhtml filtered.info --output-directory coverage_report \
       --highlight --show-details --prefix $(pwd)
```

### 7.7 ASan与覆盖率检测并行运行

两者均为编译器插桩技术，无直接冲突，可同时启用
- ASan选项​：`-fsanitize=address` + `-fno-omit-frame-pointer`
- 覆盖率选项​：`-fprofile-arcs -ftest-coverage`或 `--coverage`
链接库无冲突
- ASan需链接`-lasan`（动态库）或`-static-libasan`（静态库）
- 覆盖率需链接`-lgcov`或 `--coverage`
