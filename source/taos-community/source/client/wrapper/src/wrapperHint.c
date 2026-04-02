/*
 * Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
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

// tdengine
void tdengineErrorHint(uint32_t errCode) {
  switch (errCode) {
    case TSDB_CODE_DLL_NOT_LOAD:
      printf("Hint: Please make sure you have installed TDengine client or try re-install TDengine client.\n");
      break;
    case TSDB_CODE_DLL_FUNC_NOT_LOAD:
      printf("Hint: TDengine client libraries not matched, try re-install TDengine client.\n");
      break;
    default:
      break;
  }
}

// linux
void linuxErrorHint(uint32_t code) {
  switch (code) {
    case 2:  // ENOENT - No such file or directory
      printf("Hint: Check the following possible causes:\n");
      printf("  1. TDengine client library may not be installed. Try re-installing it.\n");
      printf("  2. Environment variables for shared library loading (e.g., LD_LIBRARY_PATH, DYLD_LIBRARY_PATH) may be misconfigured or corrupted.\n");
      break;

    case 13: // EACCES - Permission denied
      printf("Hint: Permission denied. Please check the following:\n");
      printf("  1. Verify file permissions of the shared library.\n");
      printf("  2. Check SELinux or AppArmor security policies if enabled.\n");
      break;

    case 22: // EINVAL - Invalid argument
      printf("Hint: The shared library file may be corrupted or not a valid ELF file.\n");
      printf("  Try re-installing the TDengine client library.\n");
      break;

    case 36: // ENAMETOOLONG - File name too long
      printf("Hint: The library path is too long. Try shortening the installation path.\n");
      break;

    default:
      // For other errno values, show generic message
      printf("Hint: Failed to load shared library. Error code: %u\n", code);
      printf("  Common solutions:\n");
      printf("  1. Ensure TDengine client library is properly installed.\n");
      printf("  2. Check library path configuration (LD_LIBRARY_PATH, DYLD_LIBRARY_PATH).\n");
      printf("  3. Verify system has sufficient resources (memory, file descriptors).\n");
      break;
  }
}

// windows
void winErrorHint(uint32_t code) {
  switch (code) {
    case 2:   // ERROR_FILE_NOT_FOUND
      printf("Hint: Check the following possible causes:\n");
      printf("  1. Verify TDengine client library is installed. Try re-installing it.\n");
      printf("  2. Check PATH environment variable includes the library directory.\n");
      break;
      
    case 3:   // ERROR_PATH_NOT_FOUND
      printf("Hint: The specified path was not found.\n");
      printf("  Check the installation directory path is correct.\n");
      break;
      
    case 5:   // ERROR_ACCESS_DENIED
      printf("Hint: Access denied when loading DLL.\n");
      printf("  1. Run with administrator privileges.\n");
      printf("  2. Check file permissions and antivirus software settings.\n");
      break;
      
    case 126: // ERROR_MOD_NOT_FOUND
      printf("Hint: The specified module could not be found.\n");
      printf("  1. DLL dependencies may be missing. Use 'dumpbin /dependents' to check.\n");
      printf("  2. Install Visual C++ Redistributable packages if needed.\n");
      break;
      
    case 193: // ERROR_BAD_EXE_FORMAT
      printf("Hint: The DLL is not a valid Windows application.\n");
      printf("  1. Ensure you're using the correct architecture (x86/x64).\n");
      printf("  2. Re-install TDengine client library for your platform.\n");
      break;
      
    case 998: // ERROR_NOACCESS
      printf("Hint: Invalid access to memory location.\n");
      printf("  The DLL file may be corrupted. Try re-installing.\n");
      break;
      
    default:
      printf("Hint: Failed to load DLL. Windows error code: %u\n", code);
      printf("  Common solutions:\n");
      printf("  1. Ensure TDengine client library is properly installed.\n");
      printf("  2. Check all DLL dependencies are available.\n");
      printf("  3. Verify Visual C++ Runtime is installed.\n");
      printf("  4. Run 'sfc /scannow' to check system file integrity.\n");
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
