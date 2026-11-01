"""Exercise package overlay/cleanup in temporary trees without host services."""
import os
import mmap
import platform
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
INSTALLER = HERE / "tools/install.sh"
WINDOWS = HERE.parents[1] / "taos-internal/packaging/windows/tdengine.iss"


def function(source, name):
    start = source.index("function " + name + "() {\n")
    return source[start:source.index("\n}\n", start) + 3]


class OverlayInstallTests(unittest.TestCase):
    def test_cleanup_option_is_disabled_unless_explicit(self):
        source = INSTALLER.read_text()
        options = source[source.index("imageBuild=0"):source.index("# Package archives carry")]
        for args, expected in (([], "0"), (["--clean-install"], "1")):
            result = subprocess.run(["bash", "-c", options + '\nprintf "%s" "$cleanInstall"',
                                     "installer", *args], text=True, capture_output=True, check=True)
            self.assertEqual(result.stdout, expected)


    def test_extension_rejects_clean_install_before_any_writes(self):
        import shutil

        for detected_by in ("argument", "role-marker", "manifest"):
            with self.subTest(detected_by=detected_by), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                package = root / "package"
                package.mkdir()
                shutil.copy2(INSTALLER, package / "install.sh")
                args = []
                if detected_by == "argument":
                    args = ["-m", "extension"]
                elif detected_by == "role-marker":
                    (package / ".package-role").write_text("extension\n")
                else:
                    (package / ".extension-files").write_text("plugins/fixture.bin\n")
                target = root / "target"
                target.mkdir()
                sentinel = target / "existing-file"
                sentinel.write_text("preserve me")
                before = sorted(p.relative_to(root) for p in root.rglob("*"))
                result = subprocess.run(["bash", str(package / "install.sh"),
                                         "--clean-install", "-d", str(target), *args],
                                        text=True, capture_output=True)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("--clean-install is supported only for full/core", result.stderr)
                self.assertEqual(sentinel.read_text(), "preserve me")
                self.assertEqual(sorted(p.relative_to(root) for p in root.rglob("*")), before)

    def test_optional_files_and_metadata_survive_core_overlay(self):
        for prior in ("full", "core-extension"):
            for clean in (False, True):
                with self.subTest(prior=prior, clean=clean), tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    target = root / "installed"
                    package = root / "package"
                    package.mkdir()
                    (package / ".package-info").write_text("version=new\n")
                    preserved = {
                        "plugins/plugin.bin": "plugin",
                        "connector/connector.jar": "connector",
                        "taosx/plugins/legacy.bin": "legacy plugin",
                        "driver/libdependency.so": "dependency",
                        "bin/optional-tool": "tool",
                        "examples/example.txt": "example",
                        "include/optional.h": "header",
                        "share/tsdb-docs/manual.txt": "docs",
                        ".extension-files": "plugins/plugin.bin\ndriver/libdependency.so\n",
                        ".package-role.extension": "extension\n",
                    }
                    for name, text in {**preserved, "cfg/taos.cfg": "config",
                                       "data/data.bin": "data", "log/taos.log": "log",
                                       ".package-role": "full\n", ".package-info": "old"}.items():
                        path = target / name
                        path.parent.mkdir(parents=True, exist_ok=True)
                        path.write_text(text)
                    if prior == "full":
                        (target / ".extension-files").unlink()
                        (target / ".package-role.extension").unlink()
                    source = INSTALLER.read_text()
                    script = "set -e\n" + function(source, "remove_previous_package_files") + "\n" + function(source, "install_main_path") + "\ninstall_main_path\n"
                    env = dict(os.environ, install_main_dir=str(target),
                               bin_dir=str(target / "bin"), driver_dir=str(target / "driver"),
                               configDir=str(target / "cfg"), script_dir=str(package),
                               verMode="cluster", cleanInstall=str(int(clean)))
                    subprocess.run(["bash", "-c", script], env=env, check=True)
                    for name, text in preserved.items():
                        if prior == "full" and name.startswith("."):
                            continue
                        if clean:
                            self.assertFalse((target / name).exists(), name)
                        else:
                            self.assertEqual((target / name).read_text(), text)
                    for name in ("cfg/taos.cfg", "data/data.bin", "log/taos.log"):
                        self.assertTrue((target / name).exists(), name)
                    self.assertEqual((target / ".package-info").read_text(), "version=new\n")

    def test_driver_overlay_removes_superseded_core_and_retains_extensions(self):
        for version in ("3.9", "3.4.3.3.alpha", "3.4.3.3.beta1", "3.4.3.3-rc.1", "3.4.3.3.custom-build"):
            with self.subTest(version=version):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    for name in ("package/driver", "installed/driver"):
                        (root / name).mkdir(parents=True)
                    (root / f"package/driver/libtaos.so.{version}").write_text("new")
                    (root / "package/driver/libtaos.so.3").symlink_to(f"libtaos.so.{version}")
                    (root / "installed/driver/libtaos.so.3.1").write_text("old")
                    (root / "installed/driver/libtaos.so.3.1.debug").write_text("old symbols")
                    old_prereleases = ("3.0.alpha", "3.0.beta1", "3.0-rc.1", "legacy-build")
                    for old_version in old_prereleases:
                        (root / f"installed/driver/libtaos.so.{old_version}").write_text("old prerelease")
                    (root / f"installed/driver/libtaos.so.{version}.debug").write_text("current symbols")
                    for stem in ("libtaosnative", "libtaosws", "libtaospyudf"):
                        (root / f"package/driver/{stem}.so.{version}").write_text("new")
                        (root / f"installed/driver/{stem}.so.3.1").write_text("old")
                    extensions = ("libtaos_ext_influx_arrow.so", "libcrypto.so.10", "libtaos_custom.so")
                    for name in extensions:
                        (root / "installed/driver" / name).write_text("extension")
                    source = function(INSTALLER.read_text(), "install_lib")
                    script = "set -e\noracle_write_active_client() { :; }\ninstall_fq_runtime_links() { :; }\n" + function(INSTALLER.read_text(), "copy_package_files") + "\n" + source + "\ninstall_lib\n"
                    env = dict(os.environ, script_dir=str(root / "package"), driver_dir=str(root / "installed/driver"),
                               lib_link_dir=str(root / "links"), lib64_link_dir=str(root / "links64"),
                               osType="Linux", user_mode="1")
                    subprocess.run(["bash", "-c", script], env=env, check=True)
                    self.assertEqual((root / "links/libtaos.so.3").resolve().read_text(), "new")
                    for stem in ("libtaos", "libtaosnative", "libtaosws", "libtaospyudf"):
                        self.assertFalse((root / f"installed/driver/{stem}.so.3.1").exists())
                        self.assertEqual((root / f"installed/driver/{stem}.so.{version}").read_text(), "new")
                    self.assertFalse((root / "installed/driver/libtaos.so.3.1.debug").exists())
                    for old_version in old_prereleases:
                        self.assertFalse((root / f"installed/driver/libtaos.so.{old_version}").exists())
                    self.assertEqual((root / f"installed/driver/libtaos.so.{version}.debug").read_text(), "current symbols")
                    for name in extensions:
                        self.assertEqual((root / "installed/driver" / name).read_text(), "extension")

    def test_absent_core_family_and_failed_copy_do_not_remove_existing_libraries(self):
        for failed in (False, True):
            with self.subTest(failed=failed), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                for name in ("package/driver", "installed/driver"):
                    (root / name).mkdir(parents=True)
                (root / "package/driver/liboptional.so").write_text("incoming extension")
                if failed:
                    (root / "package/driver/libtaos.so.3.9").write_text("incoming core")
                old = root / "installed/driver/libtaos.so.3.1"
                old.write_text("existing core")
                source = INSTALLER.read_text()
                script = "set -e\noracle_write_active_client() { :; }\ninstall_fq_runtime_links() { :; }\n" + function(source, "copy_package_files") + "\n" + function(source, "install_lib")
                if failed:
                    script += "\ncopy_package_files() { return 1; }\n"
                script += "\ninstall_lib\n"
                env = dict(os.environ, script_dir=str(root / "package"), driver_dir=str(root / "installed/driver"),
                           lib_link_dir=str(root / "links"), lib64_link_dir=str(root / "links64"),
                           osType="Linux", user_mode="1")
                result = subprocess.run(["bash", "-c", script], env=env, capture_output=True, text=True)
                self.assertEqual(result.returncode == 0, not failed, result.stderr)
                self.assertEqual(old.read_text(), "existing core")

    @unittest.skipUnless(platform.system() == "Linux", "requires Linux executable locking and GNU cp")
    def test_payload_overlay_replaces_running_executable_and_mapped_library(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package, target = root / "package", root / "installed"
            for directory in (package, target):
                (directory / "bin").mkdir(parents=True)
                (directory / "driver").mkdir()
            sleep = shutil.which("sleep")
            self.assertIsNotNone(sleep)
            for directory in (package, target):
                shutil.copy2(sleep, directory / "bin/daemon")
            library = target / "driver/libfixture.so"
            library.write_bytes(b"old library contents")
            (package / "driver/libfixture.so").write_bytes(b"new library contents")
            optional = target / "driver/optional.so"
            optional.write_bytes(b"retain extension")
            # The destination link must be replaced without modifying its referent.
            outside = root / "outside"
            outside.write_text("outside original")
            (target / "bin/alias").symlink_to(outside)
            (package / "bin/alias").write_text("incoming alias")
            old_inode = (target / "bin/daemon").stat().st_ino
            process = subprocess.Popen([str(target / "bin/daemon"), "60"])
            try:
                with library.open("rb") as handle, mmap.mmap(handle.fileno(), 0, access=mmap.ACCESS_READ) as mapped:
                    script = "set -e\n" + function(INSTALLER.read_text(), "copy_package_files") + '\ncopy_package_files -a "$1/." "$2/"\n'
                    subprocess.run(["bash", "-c", script, "overlay", str(package), str(target)], check=True)
                    self.assertEqual(mapped[:], b"old library contents")
                self.assertIsNone(process.poll())
                self.assertNotEqual((target / "bin/daemon").stat().st_ino, old_inode)
                self.assertEqual((target / "bin/daemon").read_bytes(), Path(sleep).read_bytes())
                self.assertEqual(library.read_bytes(), b"new library contents")
                self.assertEqual(optional.read_bytes(), b"retain extension")
                self.assertFalse((target / "bin/alias").is_symlink())
                self.assertEqual((target / "bin/alias").read_text(), "incoming alias")
                self.assertEqual(outside.read_text(), "outside original")
                (package / "bin/linked-daemon").symlink_to("daemon")
                subprocess.run(["bash", "-c", script, "overlay", str(package), str(target)], check=True)
                self.assertTrue((target / "bin/linked-daemon").is_symlink())
            finally:
                process.terminate()
                process.wait(timeout=5)

    def test_payload_copy_failure_is_not_reported_as_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            script = "set -e\n" + function(INSTALLER.read_text(), "copy_package_files") + '\ncopy_package_files "$1/missing" "$1/target"\necho unexpected-success\n'
            result = subprocess.run(["bash", "-c", script, "overlay", tmp], text=True, capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertNotIn("unexpected-success", result.stdout)

    def test_darwin_copy_does_not_receive_gnu_option(self):
        script = 'cp() { printf "%s\\n" "$@"; }\n' + function(INSTALLER.read_text(), "copy_package_files") + '\ncopy_package_files -a source target\n'
        result = subprocess.run(["bash", "-c", script], env=dict(os.environ, ostype="Darwin"),
                                check=True, text=True, capture_output=True)
        self.assertEqual(result.stdout.splitlines(), ["-a", "source", "target"])

    def test_windows_cleanup_is_opt_in_and_uninstall_still_cleans(self):
        source = WINDOWS.read_text()
        prepare = source.split("function PrepareToInstall", 1)[1].split("{ Checks whether", 1)[0]
        guard = "if ExpandConstant('{param:CLEANINSTALL|0}') = '1' then"
        self.assertIn(guard, prepare)
        for call in ("RemoveLegacyExtensionResources(InstallPath)", "RemoveExtensionResources(InstallPath)"):
            self.assertGreater(prepare.index(call), prepare.index(guard))
        self.assertIn("RemoveExtensionResources(ExpandConstant('{app}'))", source)
        self.assertIn("TaskKill('taosd.exe')", prepare)


if __name__ == "__main__":
    unittest.main()
