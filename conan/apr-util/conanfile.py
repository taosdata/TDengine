from conan import ConanFile
from conan.tools.files import get, copy
from conan.tools.gnu import Autotools, AutotoolsToolchain
import os


class AprUtilConan(ConanFile):
    name = "apr-util"
    version = "1.6.3"
    license = "Apache-2.0"
    url = "https://apr.apache.org/"
    description = "Apache APR-util"
    topics = ("apr", "apache")

    settings = "os", "compiler", "build_type", "arch"
    options = {"shared": [True, False], "fPIC": [True, False]}
    default_options = {"shared": False, "fPIC": True}

    def requirements(self):
        self.requires("apr/1.7.6")

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
            f"https://dlcdn.apache.org/apr/apr-util-{self.version}.tar.gz",
            strip_root=True,
        )

    def generate(self):
        apr_prefix = self.dependencies["apr"].package_folder

        tc = AutotoolsToolchain(self)
        tc.configure_args.append(f"--enable-shared={'yes' if self.options.shared else 'no'}")
        tc.configure_args.append("--enable-static=yes")
        tc.configure_args.append(f"--with-apr={apr_prefix}")
        tc.generate()

    def build(self):
        autotools = Autotools(self)
        autotools.configure()
        autotools.make()

    def package(self):
        copy(self, "LICENSE", src=self.source_folder, dst=os.path.join(self.package_folder, "licenses"), keep_path=False)
        autotools = Autotools(self)
        autotools.install()

    def package_info(self):
        # APR-util installs libaprutil-1
        self.cpp_info.libs = ["aprutil-1"]
        self.cpp_info.set_property("cmake_file_name", "aprutil")
        self.cpp_info.set_property("cmake_target_name", "aprutil::aprutil")
