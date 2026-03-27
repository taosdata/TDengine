# WinSW Binary Cache

This directory is used to cache the WinSW.exe binary locally.

## Usage

1. Download WinSW.exe from: [WinSW.NET461.exe](https://github.com/winsw/winsw/releases/download/v2.12.0/WinSW.NET461.exe)
2. Place it in this directory as `WinSW.exe`
3. The packaging script will use this cached version instead of downloading

## Note

- `WinSW.exe` is NOT committed to the repository (see `.gitignore`)
- If the file doesn't exist, the script will download it automatically
- The downloaded file will be cached here for future use
