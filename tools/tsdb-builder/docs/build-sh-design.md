# 统一构建脚本方案（build.sh）

> **状态**：已实施。`build.sh` 已在 `fix/refactor-tsdb-builder` 分支落地，`build-core.sh` / `build-others.sh` 保持不变作为 CI 专用脚本。

## 背景与问题

当前构建脚本存在三个痛点：

1. **组件绑定在脚本里**：修改构建目标 = 修改受版本控制的脚本，容易误提交污染 CI
2. **双脚本重复**：`build-core.sh` 和 `build-others.sh` 结构几乎一样，共同演进成本高
3. **无法按需组合**：无法轻松表达"只编译 taosx + engine"

## 方案设计

### 文件结构

```
build.sh              ← 新增：统一入口，供所有人使用
build-core.sh         ← 保留：CI 专用，内部委托给 build.sh
build-others.sh       ← 保留：CI 专用，内部委托给 build.sh
build-core-image.sh   ← 保留不变
build-others-image.sh ← 保留不变
```

### build.sh 接口

```bash
./build.sh [--arch amd64|arm64] [--image core|others|auto] <component>...
```

#### 预定义组

| 命令 | 等价组件 | 使用镜像 |
|------|---------|---------|
| `./build.sh all` | 所有组件 | others |
| `./build.sh core-all` | engine enterprise adapter keeper tools gen taosx | core |

#### 按需指定示例

```bash
./build.sh engine taosx                 # 自动选 core 镜像
./build.sh taosx explorer-ui           # 自动选 others 镜像（含 pnpm）
./build.sh insight jdbc python         # 自动选 others 镜像
./build.sh --arch arm64 engine adapter # 交叉编译指定组件
./build.sh --image others engine       # 强制用 others 镜像（开发场景）
```

### 镜像自动选择逻辑

```
请求的组件中是否含 {explorer-ui, insight, dotnet, go, jdbc, node, python, rust}？
├── 是 → tsdb-builder-others（glibc 2.28，工具全）
└── 否 → tsdb-builder-core（glibc 2.17，生产二进制兼容性最佳）
```

### CI 脚本简化后内容

```bash
# build-core.sh（保持外部接口不变，内部委托）
./build.sh --arch "$1" --image core engine enterprise adapter keeper tools gen taosx

# build-others.sh（同理）
./build.sh --arch "$1" --image others explorer-ui insight dotnet go jdbc node python rust
```

## 组件 → cmake 参数映射

| 组件名 | cmake 参数 | 默认镜像 |
|--------|-----------|---------|
| engine | BUILD_ENGINE | core |
| enterprise | BUILD_ENTERPRISE | core |
| adapter | BUILD_ADAPTER | core |
| keeper | BUILD_KEEPER | core |
| tools | BUILD_TOOLS | core |
| gen | BUILD_GEN | core |
| taosx | BUILD_TAOSX + BUILD_EXPLORER_UI=OFF | core |
| explorer-ui | BUILD_TAOSX + BUILD_EXPLORER_UI=ON | others |
| insight | BUILD_INSIGHT | others |
| dotnet | BUILD_DOTNET | others |
| go | BUILD_GO | others |
| jdbc | BUILD_JDBC | others |
| node | BUILD_NODE | others |
| python | BUILD_PYTHON | others |
| rust | BUILD_RUST | others |
| odbc | BUILD_ODBC | others |

> **注意**：`taosx` 和 `explorer-ui` 是两个独立组件名：
> - `taosx` = Rust 二进制（core 镜像，无前端）
> - `explorer-ui` = taosx Rust 二进制 + pnpm 前端（others 镜像）

## 待讨论事项

- [ ] `taosx` 与 `explorer-ui` 的组件拆分是否清晰？还是统一为 `taosx`，用 `--with-ui` 标志？
- [ ] `odbc` 默认是否应该 OFF？还是归入 others 的默认构建？
- [ ] 输出目录策略：按镜像分（`debug/` vs `debug-others/`）还是统一为一个目录？
- [ ] `.externals-${ARCH}` 隔离挂载是否需要同步到 `build.sh`（当 engine 被请求时）？
- [ ] 是否需要 `--clean` 标志来强制清理 cmake 缓存？
- [ ] CI 脚本是立即重构为委托模式，还是保持现状、只新增 `build.sh`？

## 实施顺序（待确认）

1. 实现 `build.sh`（核心逻辑）
2. 验证 `build.sh` 可替代 `build-core.sh` / `build-others.sh` 的行为
3. 重构 `build-core.sh` / `build-others.sh` 为委托调用（可选）
4. 更新 README
