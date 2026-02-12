from conan import ConanFile
from conan.tools.files import get, copy
from conan.tools.build import build_jobs
import os


class TaoswsConan(ConanFile):
    name = "taosws"
    version = "0.1.0"
    license = "MIT OR Apache-2.0"  # informational (Rust crates are typically dual licensed)
    url = "https://github.com/taosdata/taos-connector-rust"
    description = "TDengine websocket driver (libtaosws) built from taos-connector-rust"
    topics = ("tdengine", "websocket", "rust")

    settings = "os", "compiler", "build_type", "arch"

    def configure(self):
        # This package builds with Cargo; no compiler.cppstd/libcxx needed.
        self.settings.rm_safe("compiler.libcxx")
        self.settings.rm_safe("compiler.cppstd")

    def source(self):
        # Matches the default behavior of cmake/options.cmake (TAOSWS_GIT_TAG defaults to main).
        # For reproducible builds, pin this URL to a tag/commit in the future.
        get(
            self,
            "https://github.com/taosdata/taos-connector-rust/archive/refs/heads/main.tar.gz",
            strip_root=True,
        )

    def build(self):
        # Build the taos-ws-sys crate to produce libtaosws.{so,dylib,dll} and libtaosws.a
        env = {"TD_VERSION": os.environ.get("TD_VERSION", self.version)}

        # Use Rustls to avoid OpenSSL dependency (matches ExternalProject invocation)
        self.run(
            "cargo build --release --locked -p taos-ws-sys --features rustls",
            cwd=self.source_folder,
            env=env,
        )

    def package(self):
        # Headers
        copy(self, "taosws.h", src=os.path.join(self.source_folder, "target", "release"), dst=os.path.join(self.package_folder, "include"), keep_path=False)

        # Libraries
        rel = os.path.join(self.source_folder, "target", "release")
        copy(self, "libtaosws.a", src=rel, dst=os.path.join(self.package_folder, "lib"), keep_path=False)
        copy(self, "libtaosws.so*", src=rel, dst=os.path.join(self.package_folder, "lib"), keep_path=False)
        copy(self, "libtaosws.dylib*", src=rel, dst=os.path.join(self.package_folder, "lib"), keep_path=False)
        copy(self, "taosws.dll", src=rel, dst=os.path.join(self.package_folder, "bin"), keep_path=False)
        copy(self, "taosws.lib", src=rel, dst=os.path.join(self.package_folder, "lib"), keep_path=False)

    def package_info(self):
        self.cpp_info.libs = ["taosws"]
        self.cpp_info.includedirs = ["include"]
        self.cpp_info.set_property("cmake_file_name", "taosws")
        self.cpp_info.set_property("cmake_target_name", "taosws::taosws")
