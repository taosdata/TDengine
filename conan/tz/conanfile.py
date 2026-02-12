from conan import ConanFile
from conan.tools.build import build_jobs
from conan.tools.files import copy, get
import os


class TzConan(ConanFile):
    name = "tz"
    version = "2025a"
    license = "Public Domain"
    url = "https://github.com/eggert/tz"
    description = "IANA tzcode static library (libtz.a)"
    topics = ("timezone", "tzcode", "tzdb")

    settings = "os", "compiler", "build_type", "arch"
    options = {"fPIC": [True, False]}
    default_options = {"fPIC": True}

    def export_sources(self):
        # Reuse the repository's pinned tz Makefile used by ExternalProject.
        copy(
            self,
            "tz.Makefile",
            src=os.path.join(self.recipe_folder, "..", "..", "cmake", "in"),
            dst=self.export_sources_folder,
        )

    def config_options(self):
        if self.settings.os == "Windows":
            # tzcode build here is POSIX-oriented.
            raise Exception("tz package is not supported on Windows")

    def configure(self):
        # C library
        self.settings.rm_safe("compiler.libcxx")
        self.settings.rm_safe("compiler.cppstd")

    def source(self):
        get(
            self,
            f"https://github.com/eggert/tz/archive/refs/tags/{self.version}.tar.gz",
            strip_root=True,
        )

    def build(self):
        # Use the pinned Makefile (matches cmake/external.cmake behavior)
        copy(self, "tz.Makefile", src=self.export_sources_folder, dst=self.source_folder, keep_path=False)
        os.replace(os.path.join(self.source_folder, "tz.Makefile"), os.path.join(self.source_folder, "Makefile"))

        cflags = []
        if self.options.get_safe("fPIC"):
            cflags.append("-fPIC")
        cflags.append("-DTHREAD_SAFE=1")

        if self.settings.build_type == "Debug":
            cflags += ["-O0", "-g"]
        else:
            cflags += ["-O2"]

        if str(self.settings.os) == "Macos":
            # Keep consistent with external.cmake hints.
            cflags.append("-DHAVE_GETTEXT=0")

        self.run(
            f"make -j{build_jobs(self)} CFLAGS=\"{' '.join(cflags)}\" libtz.a",
            cwd=self.source_folder,
        )

    def package(self):
        # license
        copy(self, "LICENSE", src=self.source_folder, dst=os.path.join(self.package_folder, "licenses"))

        # headers (minimal)
        copy(self, "tzfile.h", src=self.source_folder, dst=os.path.join(self.package_folder, "include"))

        # library
        copy(self, "libtz.a", src=self.source_folder, dst=os.path.join(self.package_folder, "lib"), keep_path=False)

    def package_info(self):
        self.cpp_info.libs = ["tz"]
        self.cpp_info.set_property("cmake_file_name", "tz")
        self.cpp_info.set_property("cmake_target_name", "tz::tz")
