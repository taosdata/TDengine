#pragma once

#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <string>
#ifndef WINDOWS
#include <sys/stat.h>
#endif

#include "os.h"
#include "taoserror.h"

class ScopedTestTempDir {
 public:
  ScopedTestTempDir() {
#ifdef WINDOWS
    wchar_t wideParent[PATH_MAX] = {0};
    DWORD length = GetTempPathW(PATH_MAX, wideParent);
    if (length == 0 || length >= PATH_MAX) {
      code = TSDB_CODE_INVALID_PARA;
      return;
    }
    char parent[PATH_MAX] = {0};
    int bytes = WideCharToMultiByte(CP_UTF8, WC_ERR_INVALID_CHARS, wideParent, -1, parent,
                                    sizeof(parent), nullptr, nullptr);
    if (bytes == 0 || static_cast<size_t>(bytes) + kNameReserve >= sizeof(parent)) {
      code = TSDB_CODE_INVALID_PARA;
      return;
    }
    for (int attempt = 0; attempt < 32; ++attempt) {
      if (!makePath(parent)) return;
      wchar_t widePath[PATH_MAX] = {0};
      if (MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, path, -1, widePath, PATH_MAX) == 0) {
        code = TAOS_SYSTEM_WINAPI_ERROR(GetLastError());
        return;
      }
      if (CreateDirectoryW(widePath, nullptr)) {
        owned = true;
        return;
      }
      DWORD error = GetLastError();
      if (error != ERROR_ALREADY_EXISTS) {
        code = TAOS_SYSTEM_WINAPI_ERROR(error);
        return;
      }
    }
    code = TAOS_SYSTEM_WINAPI_ERROR(ERROR_ALREADY_EXISTS);
#else
    const char* parent = std::getenv("TMPDIR");
    if (parent == nullptr || parent[0] == '\0') parent = "/tmp";
    if (strlen(parent) + kNameReserve >= sizeof(path)) {
      code = TSDB_CODE_INVALID_PARA;
      return;
    }
    for (int attempt = 0; attempt < 32; ++attempt) {
      if (!makePath(parent)) return;
      if (mkdir(path, 0700) == 0) {
        owned = true;
        return;
      }
      if (errno != EEXIST) {
        code = TAOS_SYSTEM_ERROR(errno);
        return;
      }
    }
    code = TAOS_SYSTEM_ERROR(EEXIST);
#endif
  }

  ~ScopedTestTempDir() {
    if (owned) {
      taosRemoveDir(path);
      if (taosDirExist(path)) std::fprintf(stderr, "Failed to remove test temporary directory: %s\n", path);
    }
  }

  ScopedTestTempDir(const ScopedTestTempDir&) = delete;
  ScopedTestTempDir& operator=(const ScopedTestTempDir&) = delete;
  std::string child(const char* name) const { return std::string(path) + TD_DIRSEP + name; }

  char path[PATH_MAX] = {0};
  int32_t code = TSDB_CODE_SUCCESS;

 private:
  static constexpr size_t kNameReserve = 128;
  bool makePath(const char* parent) {
    char generated[PATH_MAX] = {0};
    taosGetTmpfilePath(".", "unit-test", generated);
    const std::string fullPath = std::string(parent) + TD_DIRSEP + (generated + 2);
    if (fullPath.size() >= sizeof(path)) {
      code = TSDB_CODE_INVALID_PARA;
      return false;
    }
    std::memcpy(path, fullPath.c_str(), fullPath.size() + 1);
    return true;
  }
  bool owned = false;
};
