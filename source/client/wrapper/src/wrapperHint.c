/*
 * Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "wrapperHint.h"

/* Last include on purpose: re-points the memory macros at the local libc shims. */
#include "wrapperLiteMem.h"

/* Every hint below is written to the terminal *and* mirrored into the wrapper's own
 * log file: a WebSocket-only deployment has no other place where "the driver is
 * missing" can be read after the fact, and this hint is the only explanation the
 * user gets for a failed taos_init().
 *
 * One named wrapper instead of a `#define printf`: the attribute keeps -Wformat
 * checking for every hand-formatted multi-line hint below, which the macro used to
 * switch off. */
static int tdHintPrintf(const char *format, ...)
#ifdef __GNUC__
    __attribute__((format(printf, 1, 2)))
#endif
    ;

static int tdHintPrintf(const char *format, ...) {
  char    buf[1024] = {0};
  va_list ap;
  va_start(ap, format);
  (void)vsnprintf(buf, sizeof(buf), format, ap);
  va_end(ap);

  tdLiteLogAppend(1, "WRP HINT  ", "%s", buf);
  (void)fputs(buf, stdout);
  fflush(stdout);
  return 0;
}

// tdengine
void tdengineErrorHint(uint32_t errCode) {
  switch (errCode) {
    case TSDB_CODE_DLL_NOT_LOAD:
      tdHintPrintf("Hint: Please make sure you have installed TDengine client or try re-install TDengine client.\n");
      break;
    case TSDB_CODE_DLL_FUNC_NOT_LOAD:
      tdHintPrintf("Hint: TDengine client libraries do not match, try re-installing the TDengine client.\n");
      break;
    default:
      break;
  }
}

// linux
void linuxErrorHint(uint32_t code) {
  switch (code) {
    case 2:  // ENOENT - No such file or directory
      tdHintPrintf("Hint: Check the following possible causes:\n");
      tdHintPrintf("  1. TDengine client library may not be installed. Try re-installing it.\n");
      tdHintPrintf("  2. Environment variables for shared library loading (e.g., LD_LIBRARY_PATH, DYLD_LIBRARY_PATH) may be misconfigured or corrupted.\n");
      break;

    case 13: // EACCES - Permission denied
      tdHintPrintf("Hint: Permission denied. Please check the following:\n");
      tdHintPrintf("  1. Verify file permissions of the shared library.\n");
      tdHintPrintf("  2. Check SELinux or AppArmor security policies if enabled.\n");
      break;

    case 22: // EINVAL - Invalid argument
      tdHintPrintf("Hint: The shared library file may be corrupted or not a valid ELF file.\n");
      tdHintPrintf("  Try re-installing the TDengine client library.\n");
      break;

    case 36: // ENAMETOOLONG - File name too long
      tdHintPrintf("Hint: The library path is too long. Try shortening the installation path.\n");
      break;

    default:
      // For other errno values, show generic message
      tdHintPrintf("Hint: Failed to load shared library. Error code: %u\n", code);
      tdHintPrintf("  Common solutions:\n");
      tdHintPrintf("  1. Ensure TDengine client library is properly installed.\n");
      tdHintPrintf("  2. Check library path configuration (LD_LIBRARY_PATH, DYLD_LIBRARY_PATH).\n");
      tdHintPrintf("  3. Verify system has sufficient resources (memory, file descriptors).\n");
      break;
  }
}

// windows
void winErrorHint(uint32_t code) {
  switch (code) {
    case 2:   // ERROR_FILE_NOT_FOUND
      tdHintPrintf("Hint: Check the following possible causes:\n");
      tdHintPrintf("  1. Verify TDengine client library is installed. Try re-installing it.\n");
      tdHintPrintf("  2. Check PATH environment variable includes the library directory.\n");
      break;
      
    case 3:   // ERROR_PATH_NOT_FOUND
      tdHintPrintf("Hint: The specified path was not found.\n");
      tdHintPrintf("  Check the installation directory path is correct.\n");
      break;
      
    case 5:   // ERROR_ACCESS_DENIED
      tdHintPrintf("Hint: Access denied when loading DLL.\n");
      tdHintPrintf("  1. Run with administrator privileges.\n");
      tdHintPrintf("  2. Check file permissions and antivirus software settings.\n");
      break;
      
    case 126: // ERROR_MOD_NOT_FOUND
      tdHintPrintf("Hint: The specified module could not be found.\n");
      tdHintPrintf("  1. DLL dependencies may be missing. Use 'dumpbin /dependents' to check.\n");
      tdHintPrintf("  2. Install Visual C++ Redistributable packages if needed.\n");
      break;
      
    case 193: // ERROR_BAD_EXE_FORMAT
      tdHintPrintf("Hint: The DLL is not a valid Windows application.\n");
      tdHintPrintf("  1. Ensure you're using the correct architecture (x86/x64).\n");
      tdHintPrintf("  2. Re-install TDengine client library for your platform.\n");
      break;
      
    case 998: // ERROR_NOACCESS
      tdHintPrintf("Hint: Invalid access to memory location.\n");
      tdHintPrintf("  The DLL file may be corrupted. Try re-installing.\n");
      break;
      
    default:
      tdHintPrintf("Hint: Failed to load DLL. Windows error code: %u\n", code);
      tdHintPrintf("  Common solutions:\n");
      tdHintPrintf("  1. Ensure TDengine client library is properly installed.\n");
      tdHintPrintf("  2. Check all DLL dependencies are available.\n");
      tdHintPrintf("  3. Verify Visual C++ Runtime is installed.\n");
      tdHintPrintf("  4. Run 'sfc /scannow' to check system file integrity.\n");
      break;
  }
}

// windows socket
void winSocketErrorHint(uint32_t code) {
  // nothing to do
}

// interface API
void showWrapperHint(uint32_t errCode) {
  uint32_t prefix = (errCode & 0xFFFF0000) >> 16;
  uint32_t code   =  errCode & 0x0000FFFF;

  switch (prefix) {
    case 0x8000:
      tdengineErrorHint(errCode);
      break;
    case 0x80FF:
      linuxErrorHint(code);
      break;
    case 0x81FF:
      winErrorHint(code);
      break;
    case 0x82FF:
      winSocketErrorHint(code);
      break;
    default:
      break;
  }
}
