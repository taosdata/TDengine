from conan import ConanFile
from conan.tools.files import copy
from conan.tools.scm import Git
import os


class LibGnuRxMsvcConan(ConanFile):
    name = "libgnurx-msvc"
    version = "master"
    license = "LGPL-2.1-or-later"  # informational
    url = "https://gitee.com/l0km/libgnurx-msvc"
    description = "GNU regex compatibility for MSVC"
    topics = ("regex", "windows")

    settings = "os", "compiler", "build_type", "arch"

    def configure(self):
        if self.settings.os != "Windows":
            raise Exception("libgnurx-msvc recipe is intended for Windows only")

    def source(self):
        # Gitee archive downloads can return an HTML/captcha page instead of a real .zip,
        # which breaks automated CI builds. Clone and checkout an immutable commit instead.
        commit = "1a6514dd59bac8173ad4a55f63727d36269043cd"
        git = Git(self)
        git.clone(url="https://gitee.com/l0km/libgnurx-msvc.git", target=".")
        git.checkout(commit=commit)

    def build(self):
        # Build via NMakefile as external.cmake does.
        self.run("nmake /f NMakefile all", cwd=self.source_folder)

    def package(self):
        copy(self, "regex.h", src=self.source_folder, dst=os.path.join(self.package_folder, "include"), keep_path=False)
        # The build produces regex.lib/regex_d.lib depending on config; package all libs.
        copy(self, "*.lib", src=self.source_folder, dst=os.path.join(self.package_folder, "lib"), keep_path=False)

    def package_info(self):
        self.cpp_info.libs = ["regex"]
        self.cpp_info.set_property("cmake_file_name", "libgnurx-msvc")
        self.cpp_info.set_property("cmake_target_name", "libgnurx-msvc::libgnurx-msvc")
