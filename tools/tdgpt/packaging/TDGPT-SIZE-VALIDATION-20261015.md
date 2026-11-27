# !2431 删减原则及实测（2026-10-15）

## 原则与确认状态

本方案来自本 MR 的讨论及实测。尚未取得独立研发确认记录；不能据此写成“已与研发讨论确认”。合并前应补确认人、日期和意见。

| 项目 | 最终处理 | 理由 |
|---|---|---|
| __pycache__ | 打包前删除 | 有对应源码，可重新编译；首次加载开销未量化，生产允许重新生成缓存。 |
| test/tests 目录 | 删除，但排除 include 头文件目录 | 按用户要求实际删除并做运行验证；不基于目录名宣称无风险。删除的是目录，保留正常功能的 test.py/tests.py。 |
| 普通文档、man 手册 | 删除 | 减少分发体积；保留路径或文件名含 license/licence/copyright/copying/notice 的材料。 |
| 正式 .pyi 类型提示 | 保留 | 算法运行一般不用，但用户模型开发、编辑器提示和静态类型检查会用到。tests 目录中的类型检查测试样例随测试目录删除。 |
| 系统语言包 | 保留 | 提供本地化提示，保留现有语言能力；不涉及删除 Python locale、时区数据或模型分词资源。 |
| gcc/libc-dev、Cython、pip/setuptools/wheel | 保留 | 支持用户模型编译、依赖安装与离线修复。 |
| .a 静态库和 Torch 头文件 | 保留 | 支持 C/Cython/Torch 用户扩展；导入通过不能替代编译链接验证。 |
| 模型依赖、权重、Full 镜像功能 | 保留 | 不删 JAX/pyarrow 等依赖，不量化权重，不取消 Full。 |

## test/tests 的证据与体积

此前保留测试目录是范围控制，并无证据证明删除必然破坏 TDgpt 核心算法。本轮按用户要求删除后验证。

扫描发现：SciPy special/_precompute/hyp2f1_data.py 引用 special.tests.test_hyp2f1，用途是开发校验；Statsmodels sandbox/tsa/try_fi.py 在 __main__ 示例中引用 tsa.tests。这些是具体静态引用，但不是核心运行依赖证明。Jinja2 的 tests.py 是正常功能文件，不能当作 tests 目录的删除证据。

在已发布 Linux x64 包上统计，排除 __pycache__，避免重复计算字节码收益：

| 包 | 测试文件数 | 测试文件原始字节 | 单独 gzip 归档字节 |
|---|---:|---:|---:|
| Lite venv 1.0.1 | 3990 | 62315835（59.43 MiB） | 15181201（14.48 MiB） |
| TSFM runtime 1.0.0 | 4022 | 136159664（129.85 MiB） | 38226913（36.46 MiB） |

Lite 大头来自 SciPy、Statsmodels、Pandas、NumPy、sklearn；TSFM 最大项为 Torch test 目录，约 76.64 MiB。单独压缩值不是整包删除后的精确减少量。本次两包测试目录中没有需要因 include 规则额外保留的文件；规则仍用于保护未来的头文件。

## 基线与产物

MR 原始待测提交为 a2d0933b973；联合构建使用集成提交 e1e17d03e781d32e964fe0d236d16f19ec702f4f（含其他待测 TDgpt MR），经 platform-group build_tdgpt.py --lite 生成实际 Enterprise 安装包，版本 3.4.3.1.1019。
安装包 SHA256：7af5d7ed874455eff44ea0e2c1542039cda8399447bad017f8aa37307aa338a3。
本轮后续删减原则调整只涉及构建期清理函数和 Dockerfile，安装包本身的产品安装逻辑未改；离线资源使用发布包副本加当前实际 prune_runtime 函数。

Docker 对照组为 main 的 Lite 基础 Dockerfile加上与集成分支一致的 PyOD 安装/检查；候选组为当前 MR Dockerfile。两组使用同一个产品安装包、requirements 和 Python 基础镜像。Debian 官方源阻塞后，两组统一使用仅替换 apt 源的测试基础镜像和相同 PyPI 镜像，不修改产品源码。

测量直接读取 docker save 的 OCI manifest，合计 gzip 压缩层，不采用 docker export 或缓存单独压缩估算。未推送 registry，不含配置/索引/证明元数据，不承诺等于实际网络传输量。最终依赖 freeze 与对照组完全一致，最终镜像五个 PyOD 算法、NumPy 静态链接和 HTTP /status、/list、IQR 通过。

| 镜像 | 对照压缩层字节 | 最终压缩层字节 | 减少 |
|---|---:|---:|---:|
| Lite base | 347562967 | 344666147 | 2896820 字节，2.76 MiB |
| 最终 Lite | 350284285 | 347387467 | 2896818 字节，2.76 MiB |

最终 Lite 334.06 → 331.29 MiB，约减少 0.83%。报告为 policy-image-size-final-report.json。首次测试环境曾因未合入其他 MR 的 PyOD 安装而不匹配，该次结果作废；补齐相同 PyOD 安装后重建、freeze 核对一致，并通过上述检查。

## 已完成验证

- 11 项 Docker 配置及 Python/Shell 清理回归：生成真实字节码，清理后从源码加载；test/tests、普通文档、man 删除；正式 .pyi、许可证、include/test 头文件和静态库保留；实际静态库编译、链接、运行；安装失败退出码不被清理吞掉。
- Ruff、Shell 语法和 diff 检查通过。
- 应用最终清理函数的 Lite 副本：NumPy/Pandas/SciPy 计算、Statsmodels 指数平滑、sklearn IsolationForest 和 Flask/Cython/setuptools/wheel 导入通过。
- 最终 TSFM 副本：Moirai/TimesFM/Chronos/MOMENT/transformers 导入、Torch/JAX 计算通过，不加载模型权重。
- 最终 Lite 副本在禁网环境中以 pip --no-index --no-build-isolation 构建并安装一个真实 Cython 用户扩展，执行返回值检查，再强制重新安装并执行，均通过。这只覆盖一个明确的编译/安装修复场景。
- 新安装包加最终离线 venv 在禁网容器中完成安装、版本、启动就绪、HTTP、资源列表、同版本重装、配置保留和卸载配置保留。
- 生命周期整体仍失败：卸载后 /usr/bin/taosanodectl 包装入口残留。此前已复现，不能以其他步骤通过覆盖此失败。保留容器 tdgpt-2431-policy-offline-20261015。

## 最终镜像启动与真实模型加载补测

在 192.168.2.104 的隔离容器 tdgpt-2431-model-load-20261015 中，使用最终 tdgpt-2431-policy-lite:20261015 镜像，设置 --network none。没有修改宿主机已有 TDgpt 服务或模型。

- TDgpt 主服务启动就绪，6035 /status 返回 HTTP 200、status=ready；/list 返回 HTTP 200，Moirai 状态 READY。
- 通过正式 resource import 导入最终清理后的 policy-tsfm.tar.gz 和官方 Moirai 模型包，resource verify all 通过。运行环境为 TSFM runtime 1.0.0、Python 3.12.12，使用 --python-runtime-dir /usr/local 适配镜像内 Python。
- 官方模型包 tdengine-tdgpt-model-moirai-moe-1-0-r-small.zip，433629526 字节，SHA256 9f8eeb78b701ece610cca08ace97f3ff96b9ee762c7e8232474a86fd5571c96b；测试模型 moirai-moe-1.0-r-small。导入资源列表的 model_version 字段为 null，模型名称以已校验的官方包为依据，不将该字段描述为已正确显示。
- taosanodectl model start moirai --timeout 300 成功；日志明确记录 Loading weights from local directory，模型进程监听 6064，实际加载本地权重，并非仅 import 类。
- 向模型服务 /ds_predict 发送 64 点输入、预测 4 点，返回 HTTP 200、status=success；output/lower/upper 均为 4 个有限数值。该次请求耗时 0.125 秒，仅作为单次功能验证，不代表性能基准。未覆盖从 TDgpt /forecast 入口转发的预测链路。
- 保存日志、结果后停止专用验证容器。本次只验证一个真实模型，不能据此声称全部模型通过。

证据子目录 model-load/：service-ready.log、import.log、verify.log、start-moirai.log、model-status.json、http-prediction.json、logs/model_moirai.log、container-inspect.json 及可重跑的 tdgpt-moirai-http-smoke.py。

最终清理后离线归档：Lite 167393535 字节；TSFM 443016060 字节。用 gzip level=6 重打包，不修改发布资源，也不能将这些文件直接与不同压缩参数的原发布大小相减来计算收益。

## 六模型加载及 TDgpt 转发链路实测（补充）

在同一 Linux x64 最终 Lite 镜像与清理后的 TSFM runtime 中，通过正式 resource import 导入六个官方模型包；全部包的大小和 SHA256 与发布清单一致，resource verify all 返回 valid=true。容器使用 --network none，逐个启动和停止模型，实际加载本地权重。

| 模型 | 本地权重加载 | 从 TDgpt 6035 入口实际调用 | 结果 |
|---|---|---|---|
| Moirai moirai-moe-1.0-r-small | 通过 | /forecast，64 点输入预测 4 点 | 通过 |
| Chronos chronos-bolt-base | 通过 | /forecast，64 点输入预测 4 点 | 通过 |
| TimeMoE timemoe-200m | 通过 | /forecast，64 点输入预测 4 点 | 通过 |
| TDTSFM tdtsfm-v1.0 | 通过 | /forecast，192 点输入预测 4 点 | 通过 |
| MOMENT moment-1-base | 通过 | /imputation，64 点序列删去第 31 点 | 通过，返回 64 点且缺失点 mask=1 |
| TimesFM timesfm-2.5-200m-pytorch | 通过 | /forecast，64 点输入预测 4 点 | **失败：预测值与区间为 NaN** |

预测通过项检查了 algo、rows、时间戳、结果长度及数值有限性，MOMENT 检查补值长度、数值有限性及缺失点标记；不以 HTTP 200 单独判定成功。

TDTSFM 最初采用 64 点输入返回上游错误。代码要求输入至少 96 点，调整为 192 点后通过；这是测试输入不满足模型要求，保留原始失败记录。

TimesFM 在 64/192/512 点输入下直接模型调用均产生 NaN。使用未清理的 tdgpt-2431-baseline-lite:20261015 加原始 tsfm-runtime.tar.gz、相同官方权重和输入对照，也复现 NaN，TDgpt /forecast 同样失败。因此未发现它由本 MR 清理引入，但该模型的推理验证不能算通过；尚未定位 NaN 的底层原因。

测试期间 TimesFM 首次导入返回退出码 75（资源操作锁占用），当时同时进行了 TDTSFM 的单项重试。串行重试导入成功，未将锁冲突算作模型加载问题。原始 all-model-results.json 保留首轮状态；最终判断以本表和对应 retry/192 日志为准。

证据位于 model-load/：downloads-verified.json、all-resource-verify.log、各模型 *-start.log/*-forward.json、tdtsfm-192-forward.json、timesfm-retry-forward.json、timesfm-baseline-forward.json、timesfm-policy-input-matrix.json、timesfm-baseline-input-matrix.json，以及最终模型日志和可重跑测试脚本。

## 卸载残留根因复核

残留 /usr/bin/taosanodectl 是安装器创建的普通可执行脚本，内容为调用 /var/lib/taos/taosanode/venv/bin/python3 和 /usr/local/taos/taosanode/bin/taosanodectl.py。卸载脚本只在 services 中列出 taosanoded、rmtaosanode，旧模型入口清理列表也没有 taosanodectl；随后删除产品 bin/lib，却遗漏该包装入口，导致它指向已删除的 Python 脚本。

这不是保留配置或 venv 的设计行为，也不是测试误把保留目录判为残留。当前 origin/main 的 uninstall.sh 存在相同遗漏，本次清理 MR 未修改该文件。生命周期“卸载入口清理”仍失败，不能表述为完整生命周期通过；本轮只完成根因确认，未修改卸载逻辑。保留的原容器和 uninstall-residual-wrapper 可供复查。

## 验证限制及证据

未从 requirements 完整重建离线运行包，离线验证使用发布包副本与实际清理函数。本轮已覆盖上述六个模型的权重加载与转发调用（TimesFM 推理失败）。未测 Windows、ARM、Full 镜像、所有模型的完整输入场景、全部用户自定义模型、完整修复场景和冷启动时间。现有检查通过不等于所有依赖在所有用法中均无测试目录引用。

证据：root@192.168.2.104:/data/chr/tdgpt-2431-validation-20261015/，包括 policy 构建/依赖/测量日志、源码清理函数、测试目录统计、算法/用户扩展/HTTP/离线生命周期结果。
历史实验的中间副本及镜像已清理以释放磁盘；保留日志、统计报告和最终验证所需产物。2026-10-13 报告属于此前范围的历史记录，以本报告及 MR 最新结果为准。

## 卸载入口修复及 TimesFM 输入长度复核

卸载脚本 remove_deploy_binary 中新增 rm -f 删除 bin_link_dir/taosanodectl，覆盖普通启动脚本及符号链接，保留原配置和 venv 策略。以已构建的 3.4.3.1.1019 安装包为底，仅替换 bin/uninstall.sh 后重打测试包（不是完整重跑 release 构建）。在全新禁网容器 tdgpt-2431-uninstall-fix-20261015 中，原生命周期脚本完成安装、版本、启动/HTTP、同版本重装、配置保留、卸载入口清理与卸载配置保留，最终 TDGPT_PACKAGE_LIFECYCLE_PASSED，容器退出码 0。历史残留失败记录仍保留，此次修复后该项通过。证据 uninstall-fix/lifecycle.log 和 uninstall-fix/evidence/linux-stages.log。

TimesFM 进一步对照：权重的 232 个参数张量均无非有限值；原始 runtime 中关闭 MKLDNN 仍复现短输入 NaN。使用 2048 点输入时，未清理基线和最终清理镜像经 TDgpt /forecast 均正常返回 4 点有限预测值，且两组数值一致。故不能概括为 TimesFM 全部推理失败；准确范围为 64/192/512 点短输入失败、2048 点输入通过，问题指向输入补齐相关处理，尚未确认底层原因。未因此修改预测实现。

## 正式发布 3.4.2.12 Lite 对照

从 https://downloads.tdengine.com/tdengine-tdgpt-enterprise/3.4.2.12/tdengine-tdgpt-enterprise-lite-3.4.2.12-linux-x64.tar.gz 下载正式 Linux x64 Enterprise Lite 包，196075 字节，SHA256 29579b770acfa40619fca234db873b3cc17cd918ea2cc85a9d7689f576a10c3c。未改发布包代码，在隔离禁网容器中执行其原 install.sh，配原始未清理 Lite venv 1.0.1、TSFM runtime 1.0.0、同一官方 TimesFM 2.5 200M 权重与相同 Python 3.12.12/CPU 环境。

安装和模型启动通过。TDgpt /forecast：64 点输入预测 4 点返回 NaN；2048 点输入预测 4 点通过，结果为 [10.938364028930664,10.98873519897461,10.987889289855957,10.951844215393066]，与本轮基线及候选一致。模型直接接口 64/192/512 点也均为 NaN。发布包 TimesFM 源码与当前代码相比，仅版权注释和一处路径提示文案不同，预测实现一致。

因此正式 3.4.2.12 产品代码搭配上述原始发布资源也复现相同的短输入问题。未验证其历史发布当时所用资源的完整组合，不能由此声称所有 3.4.2.12 部署都存在该问题，也不能推断此前正常测试具体用了何种输入。此次未修改预测实现。

证据 released-3.4.2.12/：download-headers.txt、install.log、import.log、timesfm-start.log、forecast-64.json、forecast-2048.json、input-matrix.json、resources.json、logs/ 和 container-inspect.json。

## 安装包整包压缩对比

对相同发布资源解包，分别保留原内容、仅删除缓存、应用最终清理规则，再以相同 tar PAX/gzip level=6 重打整个包。统一归档时间、属主及扩展元数据；只有清理范围不同，不比较原发布包的不同压缩参数。

| 安装资源包 | 清理前整包 | 最终清理后整包 | 实际减少 |
|---|---:|---:|---:|
| tdgpt-lite 的 venv 安装包 | 222.42 MiB | 159.27 MiB | **63.15 MiB（28.39%）** |
| TSFM runtime 安装包 | 555.06 MiB | 421.70 MiB | **133.36 MiB（24.03%）** |

以上减少量包含缓存、test/tests、普通文档/man。仅清缓存后的整包分别为 173.68 MiB、458.08 MiB；在此基础上继续删测试目录和普通文档/man，整包额外减少 **14.41 MiB、36.38 MiB**。这是整包差值，替代此前测试文件单独 gzip 的估算；不与 Docker 镜像减少 2.76 MiB 混用。

本次测量使用发布资源副本和实际 prune_runtime 函数，不是从 requirements 完整重建。归档规范化使其与此前未规范化重打的副本大小略有不同，以本轮同参数整包对照作为收益依据。证据 whole-package-comparison/report.json、lite/tsfm-{before,cache-only,final}.tar.gz 和 tdgpt-whole-package-compare.py；报告记录每个归档的精确字节数、SHA256 和文件数。


## tar.xz 格式切换及完整产品包复测

按本轮确认，Linux 新生成的 Python runtime、tdgpt-lite venv、模型 venv、TSFM runtime 及产品外层/内层包均使用 tar.xz；Windows EXE/ZIP 不变。安装保留历史 package.tar.gz 回退，资源导入兼容历史 gzip。历史已发布清单及其 SHA256 不做仅改后缀的替换，待新资源发布后更新真实 URL/大小/校验值。Docker save 导出包不属于产品/资源安装包，此轮不改其压缩格式。

对最终清理后的完整 gzip 资源包解压并以 XZ preset=6 重压，tar 数据流不变，解压后的 SHA256 完全一致：

| 资源包 | 同内容 gzip level=6 | XZ preset=6 | XZ 进一步减少 | XZ 重压耗时 | gzip / xz 解压校验耗时 |
|---|---:|---:|---:|---:|---:|
| tdgpt-lite 的 venv | 159.27 MiB | 98.79 MiB | 60.47 MiB（37.97%） | 246.29 s | 3.26 / 8.39 s |
| TSFM runtime | 421.70 MiB | 259.25 MiB | 162.46 MiB（38.52%） | 654.38 s | 9.16 / 22.67 s |

XZ 文件为 103591028 / 271838700 字节。压缩耗时包含读取解压 gzip，两个任务并行；解压耗时包含顺序读取及 SHA256，不含解包写盘，单次测量不作为性能保证。与清理前同参数 gzip 整包相比，最终 XZ 分别减少 123.63 MiB / 295.82 MiB。未从 requirements 完整重建这些资源。

使用集成源码 dde1510bbc001d0a2d17138b83bcbb470e1235d4，通过 platform-group build_tdgpt.py --lite 完整重跑产品 release，得到 Enterprise Lite 3.4.3.1.1019 Linux x64 tar.xz，164600 字节，SHA256 1cce9f2559f892b0eae61b48a27dab823d7132be54ee2704f135088fbef7617f；内含 package.tar.xz 和真实卸载修复。

- 构建更新后的 Lite base（含 xz-utils）及真实 DockerfileTDgpt 的 tdgpt_lite target；仅将基础镜像引用替换为独立测试 tag，使用此前相同的 apt/PyPI 镜像。
- XZ 产品包 + XZ venv 在禁网隔离容器完整通过安装、版本、启动就绪、HTTP、同版本重装、配置保留、卸载入口清理和卸载配置保留，最终退出码 0。此前旧测试基础镜像没有 xz，解包失败；更新后的基础镜像复测通过。
- 新产品镜像默认入口启动通过；正式 resource import 导入 XZ TSFM runtime 和官方 Moirai ZIP，resource verify all 为 valid=true；实际加载 Moirai 权重，从 TDgpt /forecast 用 64 点预测 4 点，长度、时间戳及有限值检查通过。新版镜像 HTTP /status、/list、IQR 和 NumPy 静态库编译链接通过，pip freeze 与此前最终镜像一致。验证结束停止专用容器。
- 新镜像 OCI gzip 压缩层：Lite base 344835436 字节，产品镜像 347514487 字节。相对此前最终镜像 347387467 字节增加 127020 字节，包含 xz-utils 与本轮产品变化；相对原始对照 350284285 字节减少约 2.64 MiB（0.79%），不将 XZ 资源压缩收益计入镜像收益。
- TSDB 清理/真实 XZ 归档/资源导入回归 39 项通过；platform-group 包名、官网元数据及 Apex 发布清单回归 41 项通过。平台 plumbing 的 TDgpt 子集 37 passed / 13 failed，与未修改 HEAD 的失败名称一致；变更文件 Ruff 的 53 个既有问题与 HEAD 相同，无新增。

本轮未实测 ARM、Full 镜像、Windows 重构建；六模型完整验证见上节，本次格式变更对模型加载/转发补测 Moirai。证据 xz-integration/、whole-package-comparison/xz-report.json 和 xz-readback-report.json；未上传正式资源、未发布镜像。


## 取消内层归档（最终结构）

后续按用户要求取消内层 package.tar.xz。release 直接保留 lib/taosanalytics，只有产品外层 tar.xz 压缩；安装器检查 lib/taosanalytics 后直接安装，不再寻找或解压任何 package.tar.*。历史 gzip 资源导入能力不变；旧产品安装包自带其原安装脚本，本次不将新脚本用于旧产品布局。

使用前述集成构建工作区同步最新 release.sh/install.sh，再经 platform-group 完整重建相同版本。最终产品 158952 字节，SHA256 59c1e9ac95e662a39669c85aa383bc2b248b5a707f59fd3f1b43741cba2f7445，比此前双层 XZ 包少 5648 字节。文件清单确认直接包含 lib/taosanalytics/__init__.py、没有 package.tar.*。

禁网生命周期再次通过安装、版本、启动/HTTP、同版本重装、配置保留、卸载入口清理和配置保留（退出码 0）；真实 Lite Docker target 再次构建成功，默认入口 HTTP /status、/list、IQR 通过。相关 Docker/清理回归 12 passed，Shell 语法通过。资源包内容未修改，不重复六模型测试。证据 xz-integration/direct-{build,lifecycle,image-build,http}.log、direct-file-list.txt、direct-evidence/。本节取代上节内层 XZ 结构及产品校验值。

取消内层包后的最终产品镜像重新测量 OCI gzip 压缩层为 **347508857 字节（331.41 MiB）**，相对原始对照 350284285 字节减少 2775428 字节，约 **2.65 MiB（0.79%）**。证据 xz-integration/direct-image-size-report.json。此前 347514487 字节属于双层 XZ 产品布局，以本轮值为准。


## Windows x64 主 venv 清理与实测

按用户要求，将相同清理原则同步到 build_windows_lite_venv.py：构建依赖后删除缓存、依赖库 test/tests 和普通 share/doc/share/man，保留法律材料、正式 .pyi、静态/导入库及 Include/include 下的头文件；Windows 头文件路径判断不区分大小写。清理后运行 pip check、主依赖导入，写入 lock/manifest，最终再清理一次后生成 ZIP。Windows 产品继续 EXE、资源继续 ZIP。

原生验证机 administrator@192.168.2.74，隔离构建目录 D:\cd-test\tdgpt-pruning-20261015。使用集成源码 18b942667caa5af2d905d14af463184687b1dc7f 的 requirements_docker.txt 和本 MR 新构建脚本，从 requirements 完整新建 venv，Python runtime 3.12.12，PyPI 镜像为清华源。产品 EXE 从同一集成源码通过 win_release.py --lite 完整构建，版本 3.4.3.1.1019。

| Windows 主 venv ZIP | 字节 | MiB |
|---|---:|---:|
| 同一次依赖安装，清理前 | 260222924 | 248.17 |
| 清理后 | 179682516 | 171.36 |
| 整包减少 | 80540408 | **76.81（30.95%）** |

ZIP 对照使用相同 package_venv / ZIP_DEFLATED 和同一目录，不与旧发布资源不同依赖组合相减。最终 ZIP SHA256：28db144c35090127f6526d0261d6f5bc7993d8fcdf8c54fe5d2d43e1782cfbc4。删减未压缩内容：缓存约 153.00 MiB、test/tests 约 65.16 MiB、doc/man 约 5.5 KiB。正式 .pyi 约 1.03 MiB、静态/导入库约 14.97 MiB，清理前后对应路径的大小和 ZIP CRC 完全一致；pip freeze --all 一致，pip check 通过。扫描最终 ZIP 确认无缓存/test/tests。

- 真实 EXE 配最终清理 ZIP，专用测试机完成已有安装升级、卸载、全新安装、同版本重装；默认编码 -V、SCM 显示名、服务 wait-ready、HTTP /status、/list、IQR、资源表版本列和配置保留全部通过，report.json status=passed、failures=[]。EXE SHA256：c944c58de2d5a3bba12c3fa1885c4a74d9c01ab80c13be23b063bdd9d2631758。
- 在构建 venv 和实际安装后的 venv 中，NumPy/SciPy 计算、Statsmodels 指数平滑、sklearn IsolationForest、Numba JIT、StatsForecast AutoETS 的实际计算均通过，检查结果长度和有限值。未将算法列表可见等同于执行通过。
- 原生 Windows 合成用例确认 Include/tests/model.h 与 .lib 保留、普通依赖 tests 删除。真实清理前 ZIP 没有 Include 下测试头文件，因此补充大小写保护不改变本次实测 ZIP 内容。
- 本地相关回归 76 passed，含 Windows 清理及头文件保留、验证后再次清缓存；Ruff 和 diff 检查通过。验证结束 Taosanode 服务停止，保留测试安装、资源和证据。未上传 Windows 资源到发布目录。

本轮只扩展 Windows 主 venv 与安装生命周期验证；未重新构建 Windows TSFM runtime、加载其模型权重，也未验证原生 Windows C/Cython 编译安装。Windows 结果替代上文“Windows 主 venv 未新增清理/未测 Windows”中相应历史限制；ARM、Full 等范围仍以各节说明为准。

证据：D:\cd-test\tdgpt-pruning-20261015\ 下 build-report.json、archive-audit.json、build.log、product-build.log、lifecycle/report.json、各阶段安装/版本/服务日志、algorithm-smoke.log、installed-algorithm-smoke.log、header-check.log。


## 主 venv 1.0.2 资源发布

Linux 和 Windows 主 venv 资源版本统一为 1.0.2，旧资源保留。
发布目录：`192.168.2.104:/pkgs/release-apex/tdengine-tdgpt-enterprise/deps/`，
公开前缀：`https://release.tdengine.net/tdengine-tdgpt-enterprise/deps/`。

| 文件 | 字节 | SHA256 |
| --- | ---: | --- |
| `tdengine-tdgpt-lite-venv-1.0.2-linux-x64.tar.xz` | 103591028 | `6ab0a92a40c83482bd56af9500b69f8d6b9ae378fdaad95cc1390f8a3e03cbe9` |
| `tdengine-tdgpt-lite-venv-1.0.2-linux-arm64.tar.xz` | 94859088 | `b55598d55d9a982cb10a68cb129c4d57d36ffc6341d6f287d9547d6625f95f86` |
| `tdengine-tdgpt-lite-venv-1.0.2-windows-x64.zip` | 179682511 | `69b824ae32898b4e1acbbbdd9b59ac9915373704a151b30cb9166ef7bf822653` |

三平台公开 URL 均完整下载并核对 SHA256 一致。Linux x64 复用此前通过实测的清理后
XZ 内容，包内无资源版本 manifest；不宣称本次从 requirements 重建。Windows 基于本轮完整
重建的清理后 ZIP，仅更新 manifest 的资源版本为 1.0.2，其他归档成员 CRC 与大小一致。
产品版本仍为 3.4.3.1.1019，与资源版本独立。Windows 1.0.2 新资源的升级、卸载、全新安装、重装、默认编码版本输出、服务就绪及 HTTP 全部通过，测试结束 Taosanode 停止。复测证据：
`192.168.2.74:D:\cd-test\tdgpt-pruning-20261015\lifecycle-102\`。
platform-group !474 同步 E2E 默认版本及下次发版的 Apex 清单 URL、版本、哈希与安装路径。

发布前须同步至生产下载源；本轮新 Linux x64 URL 在 downloads.tdengine.com / downloads.taosdata.com 仍为 404，合并清单不会自动完成资源同步。

ARM64 主 venv 1.0.2 已在原生 aarch64 机器 `192.168.3.91` 从当前 requirements 完整构建，
XZ 归档 **94859088 字节**，SHA256 `b55598d55d9a982cb10a68cb129c4d57d36ffc6341d6f287d9547d6625f95f86`。
归档完整性、解压后 pip check、NumPy/SciPy、Statsmodels、sklearn、StatsForecast 实算，
以及当前 TDgpt 源码在 Gunicorn 下启动、HTTP /status、/list、IQR 请求通过。
本次没有原生安装/卸载、systemd 或模型权重推理覆盖；不等同于 ARM 全生命周期认证。
证据：`192.168.3.91:/data/chr/tdgpt-venv-1.0.2-20261015/`（build.log、pip-check.log、smoke.log、app-smoke.log）。

三平台 1.0.2 资源已发布至同一 deps 目录；下载地址、大小和 SHA256 汇总：[资源清单](https://release.tdengine.net/tdengine-tdgpt-enterprise/deps/tdengine-tdgpt-lite-venv-1.0.2.json)。
