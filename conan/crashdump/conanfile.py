from conan import ConanFile
from conan.tools.cmake import CMake, CMakeToolchain, cmake_layout
from conan.tools.files import get, save
import os


_CRASHDUMP_CMAKELISTS = r"""
cmake_minimum_required(VERSION 3.16)
project(crashdump C)

add_executable(dumper dumper/dumper.c)
target_link_libraries(dumper Dbghelp)
install(TARGETS dumper RUNTIME DESTINATION bin)

add_library(crashdump STATIC crasher/crasher.c)
install(TARGETS crashdump ARCHIVE DESTINATION lib)
"""

# Upstream repo is an archived demo project and doesn't ship a reusable header.
# TDengine only needs `exceptionHandler` (declared in source/os/src/osSysinfo.c) and
# the helper dumper.exe. Provide a patched crasher.c without a `main()`.
_CRASHER_C = r"""
#include <Windows.h>

#include <stdio.h>
#include <string.h>
#include <wchar.h>

LONG WINAPI exceptionHandler(LPEXCEPTION_POINTERS exception)
{
  wchar_t currentProcessPath[MAX_PATH + 1];
  wchar_t dumperProcessPath[MAX_PATH + 1];

  DWORD written = GetModuleFileNameW(NULL, currentProcessPath, sizeof(currentProcessPath) / sizeof(currentProcessPath[0]));
  for (DWORD i = written - 1; i >= 0; i--)
  {
    if (currentProcessPath[i] == L'\\')
    {
      memcpy_s(dumperProcessPath, sizeof(dumperProcessPath), currentProcessPath, i * sizeof(i));
      wchar_t dumperProcessName[] = L"\\dumper.exe";
      memcpy_s(dumperProcessPath + i, sizeof(dumperProcessPath) - i * sizeof(i), dumperProcessName, sizeof(dumperProcessName));
      break;
    }
  }

  wchar_t applicationName[MAX_PATH + 3];
  wchar_t commandLine[32768];
  swprintf_s(applicationName, sizeof(applicationName) / sizeof(applicationName[0]), L"\"%ls\"", dumperProcessPath);
  swprintf_s(commandLine, sizeof(commandLine) / sizeof(commandLine[0]), L"\"%ls\" %lu %lu %p", dumperProcessPath, GetCurrentProcessId(), GetCurrentThreadId(), exception);

  STARTUPINFOW startupInfo = { 0 };
  startupInfo.cb = sizeof(STARTUPINFOW);
  PROCESS_INFORMATION dumperProcessInfo;
  if (CreateProcessW(dumperProcessPath, commandLine, NULL, NULL, FALSE, NORMAL_PRIORITY_CLASS, NULL, NULL, &startupInfo, &dumperProcessInfo) == 0)
  {
    return FALSE;
  }

  // Wait to be terminated by dumper. This may look like it's resilient to the dumper crashing, but it isn't.
  // The dumper will have suspended this thread, so if it crashes without resuming it this process will stay stuck forever.
  WaitForSingleObject(dumperProcessInfo.hProcess, INFINITE);

  CloseHandle(dumperProcessInfo.hProcess);
  CloseHandle(dumperProcessInfo.hThread);

  return TRUE;
}
"""


class CrashdumpConan(ConanFile):
    name = "crashdump"
    version = "master"
    license = "MIT"  # informational
    url = "https://github.com/Arnavion/crashdump"
    description = "Windows crash dump helper"
    topics = ("crashdump", "windows")

    settings = "os", "compiler", "build_type", "arch"

    def configure(self):
        if self.settings.os != "Windows":
            raise Exception("crashdump recipe is intended for Windows only")

    def layout(self):
        cmake_layout(self)

    def source(self):
        get(
            self,
            "https://github.com/Arnavion/crashdump/archive/149b43c10debdf28a2c50d79dee5ff344d83bd06.tar.gz",
            strip_root=True,
        )

    def generate(self):
        tc = CMakeToolchain(self)
        tc.generate()

    def build(self):
        save(self, os.path.join(self.source_folder, "CMakeLists.txt"), _CRASHDUMP_CMAKELISTS)

        # Patch upstream demo source into a reusable library object for TDengine.
        save(self, os.path.join(self.source_folder, "crasher", "crasher.c"), _CRASHER_C)

        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        cmake = CMake(self)
        cmake.install()

    def package_info(self):
        self.cpp_info.libs = ["crashdump"]
        self.cpp_info.system_libs = ["Dbghelp"]
        self.cpp_info.set_property("cmake_file_name", "crashdump")
        self.cpp_info.set_property("cmake_target_name", "crashdump::crashdump")
