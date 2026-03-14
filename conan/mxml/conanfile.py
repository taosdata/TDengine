from conan import ConanFile
from conan.tools.files import get, copy
from conan.tools.gnu import Autotools, AutotoolsToolchain
import os


class MxmlConan(ConanFile):
    name = "mxml"
    version = "2.12"
    license = "Apache-2.0"  # informational
    url = "https://github.com/michaelrsweet/mxml"
    description = "Mini-XML library"
    topics = ("xml", "mxml")

    settings = "os", "compiler", "build_type", "arch"
    options = {"shared": [True, False], "fPIC": [True, False]}
    default_options = {"shared": False, "fPIC": True}

    def config_options(self):
        if self.settings.os == "Windows":
            del self.options.fPIC

    def configure(self):
        if self.options.shared:
            self.options.rm_safe("fPIC")
        self.settings.rm_safe("compiler.libcxx")
        self.settings.rm_safe("compiler.cppstd")

    def source(self):
        get(
            self,
            f"https://github.com/michaelrsweet/mxml/archive/refs/tags/v{self.version}.tar.gz",
            strip_root=True,
        )

    def generate(self):
        tc = AutotoolsToolchain(self)
        tc.configure_args.append(f"--enable-shared={'yes' if self.options.shared else 'no'}")
        tc.configure_args.append("--enable-static=yes")
        tc.generate()

    def build(self):
        autotools = Autotools(self)
        autotools.configure()
        autotools.make()

    def package(self):
        copy(self, "LICENSE*", src=self.source_folder, dst=os.path.join(self.package_folder, "licenses"), keep_path=False)

        autotools = Autotools(self)
        autotools.install()

    def package_info(self):
        self.cpp_info.libs = ["mxml"]
        self.cpp_info.set_property("cmake_file_name", "mxml")
        self.cpp_info.set_property("cmake_target_name", "mxml::mxml")
