# 打包工具二进制缓存

本目录保存 Windows 打包脚本使用的本地辅助工具。

## 文件

- `WinSW.exe`
  供 `packaging/win_release.py` 作为本地 WinSW 缓存使用。
- `uv.exe`
  未传入 `--python-runtime-dir` 时，供 `packaging/build_offline_assets.py`
  安装或下载 Python runtime。

## WinSW 行为

打包时按以下顺序处理：

1. `win_release.py` 首先检查 `packaging/bin/WinSW.exe`。
2. 如果文件存在，则将它复制到安装包 staging 目录。
3. 在安装包 staging 内容中，将它重命名为 `taosanode-winsw.exe`。
4. 如果缓存文件不存在，脚本会从 GitHub 下载 WinSW，并将下载结果保存到本目录，
   供后续打包复用。

下载来源：

- [WinSW.NET461.exe](https://github.com/winsw/winsw/releases/download/v2.12.0/WinSW.NET461.exe)

## `uv` 行为

构建离线资源时按以下顺序查找：

1. `build_offline_assets.py` 首先检查 `packaging/bin/uv.exe`。
2. 如果传入 `--uv-exe`，则优先使用参数指定的路径。
3. 如果两处都没有可用文件，则回退到 `where uv` 查找宿主机上的 `uv`。

## 说明

- 这些文件仅供构建阶段使用，不是目标机器上的最终安装位置。
- 将辅助工具保存在本目录，可以避免每次打包时重复下载或查找宿主机工具。
