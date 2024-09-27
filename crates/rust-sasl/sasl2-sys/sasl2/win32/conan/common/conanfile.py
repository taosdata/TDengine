from conans import ConanFile, MSBuild

# This is a common library used in every other subproject of cyrus-sasl
# Even though cyrus-sasl-core.sln builds its own copy of this library
# making it possible to build static cyrus-sasl while this one is 
# supposed to be used nly with dynamic runtimes (for dynamic plugins).
class CyrusSaslCommonConan(ConanFile):
    version = "2.1.26"
    license = "BSD-with-attribution"
    url = "https://github.com/Ri0n/cyrus-sasl.git"
    settings = "os", "compiler", "build_type", "arch"
    exports_sources="../../../*"

    name = "cyrus-sasl-common"
    description = "Cyrus SASL internal common library"
    options = {"shared": [False]}
    default_options = "shared=False"
    requires = "OpenSSL/1.0.2o@conan/stable"

    def build(self):
        #replace_in_file("win32\\openssl.props", "libeay32.lib;", "")
        msbuild = MSBuild(self)
        msbuild.build_env.runtime = ["MD","MDd"][self.settings.get_safe("build_type") == "Debug"]
        msbuild.build("win32\\cyrus-sasl-common.sln")

    def package(self):
        self.copy("*common*.lib", dst="lib", keep_path=False)
        self.copy("*common*.a", dst="lib", keep_path=False)

    def package_info(self):
        self.cpp_info.libs = ["libcommon.lib"]

