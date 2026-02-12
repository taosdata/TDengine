from conan import ConanFile
from conan.tools.files import get, copy, load, save
import os
import re


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

    def _upstream_ref(self) -> str:
        """Return upstream git ref/commit for taos-connector-rust.

        Priority:
        1) TAOSWS_GIT_COMMIT env var (manual pin)
        2) conandata.yml: sources.commit (injected by build.sh during `conan export`)
        3) fallback: main
        """
        env_commit = os.getenv("TAOSWS_GIT_COMMIT")
        if env_commit:
            return env_commit.strip()

        try:
            commit = self.conan_data.get("sources", {}).get("commit")
            if commit:
                return str(commit).strip()
        except Exception:
            pass

        return "main"

    def source(self):
        ref = self._upstream_ref()
        if re.fullmatch(r"[0-9a-fA-F]{40}", ref):
            # Prefer commit tarball for reproducibility.
            url = f"https://github.com/taosdata/taos-connector-rust/archive/{ref}.tar.gz"
        else:
            # Fallback to main branch tarball.
            url = "https://github.com/taosdata/taos-connector-rust/archive/refs/heads/main.tar.gz"

        get(self, url, strip_root=True)

        # Persist for later packaging (and for debugging package provenance).
        save(self, os.path.join(self.source_folder, ".taosws_upstream_ref"), ref)

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
        copy(
            self,
            "taosws.h",
            src=os.path.join(self.source_folder, "target", "release"),
            dst=os.path.join(self.package_folder, "include"),
            keep_path=False,
        )

        # Libraries
        rel = os.path.join(self.source_folder, "target", "release")
        copy(self, "libtaosws.a", src=rel, dst=os.path.join(self.package_folder, "lib"), keep_path=False)
        copy(self, "libtaosws.so*", src=rel, dst=os.path.join(self.package_folder, "lib"), keep_path=False)
        copy(self, "libtaosws.dylib*", src=rel, dst=os.path.join(self.package_folder, "lib"), keep_path=False)
        copy(self, "taosws.dll", src=rel, dst=os.path.join(self.package_folder, "bin"), keep_path=False)
        copy(self, "taosws.lib", src=rel, dst=os.path.join(self.package_folder, "lib"), keep_path=False)

        # Record upstream ref/commit used for this build.
        try:
            ref = load(self, os.path.join(self.source_folder, ".taosws_upstream_ref")).strip()
        except Exception:
            ref = self._upstream_ref()
        save(self, os.path.join(self.package_folder, "res", "taosws_upstream_ref.txt"), ref)

    def package_info(self):
        self.cpp_info.libs = ["taosws"]
        self.cpp_info.includedirs = ["include"]
        self.cpp_info.set_property("cmake_file_name", "taosws")
        self.cpp_info.set_property("cmake_target_name", "taosws::taosws")
