# Packaging Binary Cache

This directory stores local helper binaries used by the Windows packaging scripts.

## Files

- `WinSW.exe`
  Used by `packaging/win_release.py` as the local WinSW cache.
- `uv.exe`
  Used by `packaging/build_offline_assets.py` to install/download a Python runtime when `--python-runtime-dir` is not provided.

## WinSW Behavior

During packaging:

1. `win_release.py` first checks `packaging/bin/WinSW.exe`
2. If the file exists, it is copied into the staged installer payload
3. Inside the installer payload, it is renamed to `taosanode-winsw.exe`
4. If the cache file does not exist, the script downloads WinSW from GitHub and stores it here for later reuse

Download source:

- [WinSW.NET461.exe](https://github.com/winsw/winsw/releases/download/v2.12.0/WinSW.NET461.exe)

## uv Behavior

During offline asset packaging:

1. `build_offline_assets.py` first checks `packaging/bin/uv.exe`
2. If `--uv-exe` is provided, that path takes precedence
3. If neither is available, the script falls back to `where uv`

## Notes

- These files are build-time helpers, not the final installed location on the target machine
- Keeping them here avoids repeated external downloads or host tool lookups during packaging
