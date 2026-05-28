---
name: tsdb-test-pytest-standard
description: "TDengine Python 测试用例编写规范。定义 test/cases/ 下 Python 测试用例的标准结构、命名规范、类组织方式、docstring 格式和 CI 注册流程。关键词: Python test, pytest, test case, TDengine testing, 测试用例"
metadata:
  author: kjduan
  version: 1.0.0
  owner_team: engine
---

# TDengine Python Test Case Standard

## Overview

This skill defines the standard for writing TDengine Python test cases under `test/cases/`.
Use `case.py` in this directory as a copy-paste template.

---

## File & Class Rules

- One class per file. Class name is PascalCase matching the file name (e.g. `test_foo_bar.py` → `class TestFooBar`).
- Only `test_` prefixed methods are pytest entry points. All others are helpers.
- File must start with the required import line; add extras only when needed.

```python
from new_test_framework.utils import tdLog, tdSql, etool
```

---

## Class Structure

```
class TestXxx:
    setup_class          # one-time init (deploy config, global state)
    # --- util ---
    helper methods       # reusable checks / builders (no test_ prefix)
    # --- impl ---
    do_* methods         # one logical test group each; ends with print("... [passed]")
    # --- main ---
    test_* methods       # pytest entry; calls do_* only, no inline logic
```

---

## `setup_class`

```python
def setup_class(cls):
    tdLog.debug(f"start to execute {__file__}")
    # one-time setup: create users, databases, etc.
```

---

## `test_*` Docstring Format (mandatory)

Every `test_` function must have this docstring:

```python
def test_something(self):
    """Short one-line description

    1. What the first group of tests covers
    2. What the second group covers
    ...

    Catalog:
        - CategoryName

    Since: vX.Y.Z.W

    Labels: common,ci[,other]

    Jira: TD-XXXXX or None

    History:
        - YYYY-MM-DD Author Description

    """
```

Fields:
| Field     | Description |
|-----------|-------------|
| `Catalog` | Feature area (e.g. User, Stream, Query) |
| `Since`   | First version this case applies to |
| `Labels`  | `common` always included; add `ci` for CI-required cases |
| `Jira`    | Ticket number or `None` |
| `History` | One line per significant change, newest last |

---

## `test_*` Body Rule

The body calls `do_*` methods only — no SQL, no assertions inline:

```python
def test_something(self):
    """..."""
    self.prepare()
    self.do_first_scenario()
    self.do_second_scenario()
```

---

## `do_*` Methods

Each covers one logical scenario. End with a progress print:

```python
def do_something(self):
    # normal path
    tdSql.execute("CREATE ...")
    tdSql.query("SELECT ...")
    tdSql.checkRows(1)

    # exception path
    tdSql.error("CREATE ... invalid")

    print("something ......................... [ passed ]")
```

---

## SQL Execution API (`tdSql`)

| Method | Use |
|--------|-----|
| `tdSql.execute(sql)` | DDL / DML that must succeed |
| `tdSql.query(sql)` | SELECT; result stored in `tdSql` |
| `tdSql.error(sql)` | Assert SQL must fail |
| `tdSql.checkRows(n)` | Assert row count after query |
| `tdSql.checkData(row, col, val)` | Assert cell value |
| `tdSql.checkKeyData(key, col, val)` | Assert cell by key column |
| `tdSql.getFirstValue(sql)` | Execute query, return first cell |
| `tdSql.getData(row, col)` | Get cell value after query |
| `tdSql.connect(user, password)` | Switch connection |

---

## Logging

```python
tdLog.debug("message")   # debug detail
tdLog.info("message")    # step marker, e.g. "=== step1: create user"
```

---

## External Tools (`etool`)

Use `etool` for shell-level operations (running `taos` CLI, file paths, etc.):

```python
taosFile = etool.taosFile()
result = etool.runRetList(command, checkRun=True, show=True)
```

---

## Comments

- English only, concise.
- Mark logical sections with `# --- section name ---`.
- Inline comments only where logic is non-obvious.
- No docstrings on helper methods unless the signature alone is unclear.

---

## Running Tests Locally

### Normal pytest run

From `test/` directory:

```bash
cd test
pytest cases/24-Users/test_user_token.py
pytest cases/24-Users/test_user_token.py::TestUserToken::test_user_token   # single method
pytest cases/24-Users/test_user_token.py -v                                # verbose
```

### With ASAN / memory-leak detection (pytest.sh)

Use `./ci/pytest.sh` as the wrapper — it sets up `LD_PRELOAD` for AddressSanitizer and checks the ASAN output after the run:

```bash
cd test
./ci/pytest.sh pytest cases/24-Users/test_user_token.py
```

Flags passed after the file name are forwarded to pytest:

```bash
./ci/pytest.sh pytest cases/81-Tools/06-TaosBackup/test_taosbackup_basic.py -B   # -B = backup mode
./ci/pytest.sh pytest cases/01-DataTypes/test_datatype_bigint.py -N 3            # -N = node count
```

ASAN results are written to `sim/asan/psim.info`. The script exits non-zero if the case did not print `successfully executed`.

---

## Adding a Case to CI (`test/ci/cases.task`)

CI reads `cases.task` line by line. Each non-comment line has five comma-separated fields:

```
priority,rerunTimes,sanitizer,workDir,command
# example:
,,y,.,./ci/pytest.sh pytest cases/24-Users/test_user_token.py
```

| Field | Typical value | Meaning |
|-------|---------------|---------|
| priority | (empty) | execution priority, leave blank for default |
| rerunTimes | (empty) | auto-retry count on failure, leave blank |
| sanitizer | `y` | run with ASAN (`y` = yes) |
| workDir | `.` | working directory (always `.`) |
| command | `./ci/pytest.sh pytest cases/...` | the command CI runs |

Steps to register a new case:

1. Find the relevant section comment in `cases.task` (e.g. `## 24-Users`).
2. Append a line in the same format as its neighbours.
3. Commit the change together with the new test file.

To temporarily disable a case without deleting it, prefix the line with `#`:

```
#,,y,.,./ci/pytest.sh pytest cases/24-Users/test_user_token.py
```

## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-test-pytest-standard version=1.0.0 author=kjduan`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->
