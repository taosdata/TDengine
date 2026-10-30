# Copyright (c) TAOS Data, Inc. All rights reserved.
#
# SPDX-License-Identifier: LicenseRef-TAOSData-Commercial
#
# This file is proprietary to TAOS Data, Inc. and may contain confidential
# information. No rights are granted by this file. Use is permitted only
# under a written agreement with TAOS Data, Inc. or an authorized affiliate.
# Unauthorized copying, modification, distribution, or disclosure, in whole
# or in part, by any medium, is strictly prohibited.

"""Regression checks for image installation and legacy configuration normalization."""
import platform
import subprocess
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
INSTALLER = HERE.parents[2] / "tools/install.sh"


class ImageBuildTests(unittest.TestCase):
    @unittest.skipUnless(platform.system() == "Linux", "Installer config functions require GNU sed; validated on Linux")
    def test_installer_options_and_address_rewrites(self):
        source = INSTALLER.read_text()
        options = source[source.index("imageBuild=0"):source.index("# Package archives carry")]
        functions = source[source.index("function install_taosx_config()"):source.index("function install_taosinspect_config()")]
        for args, image in [("--image-build -e yes", True), ("-e no", False), ("-s", False)]:
            with self.subTest(args=args), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                (root / "cfg").mkdir()
                (root / "taosx/etc/taos").mkdir(parents=True)
                (root / "out").mkdir()
                (root / "cfg/taos.cfg").write_text("# fqdn default-host\n")
                for name in ("explorer", "taosadapter", "taoskeeper"):
                    (root / f"cfg/{name}.toml").write_text('host = "localhost"\nother = "127.0.0.1"\n')
                (root / "taosx/etc/taos/taosx.toml").write_text('[monitor]\n#fqdn = "localhost"\n')
                script = "set -e\n" + options + functions + """
script_dir=$1
configDir=$script_dir/out
configFile=taos.cfg
PREFIX=taos
xname=taosx
explorerName=explorer
adapterName=taosadapter
keeperName=taoskeeper
serverFqdn=build-host
user_mode=1
taos_dir_set=0
verMode=edge
dataDir=/var/lib/taos
logDir=/var/log/taos
get_config_file() { echo explorer.toml; }
install_taosd_config
install_taosx_config
install_explorer_config
install_adapter_config
install_keeper_config
"""
                # Parameter parsing consumes installer arguments; reset positional args for fixtures.
                script = script.replace("script_dir=$1", 'script_dir="' + tmp + '"')
                subprocess.run(["bash", "-c", script, "installer", *args.split()], check=True)
                output = "".join(p.read_text() for p in (root / "out").iterdir())
                self.assertEqual("build-host" in output, not image)
                self.assertIn("monitor 1", output)
                self.assertIn("dataDir /var/lib/taos", output)
                if image:
                    self.assertIn("monitorFQDN localhost", output)

    def test_image_build_skips_service_and_process_operations(self):
        source = INSTALLER.read_text()
        functions = ""
        for name in ("install_services", "install_service", "kill_process", "updateProduct"):
            start = source.index("function " + name + "() {\n")
            end = source.index("\n}\n", start) + 3
            functions += source[start:end] + "\n"
        # Unexpected service/process calls fail. Exercise fresh service setup,
        # direct calls and the upgrade path with a simulated running server.
        script = "set -e\n" + functions + """
imageBuild=1
services=(taosd taosx)
service_mod=0
serverName=taosd
sysctl_cmd=unexpected
osinfo=ubuntu
packageRole=full
verMode=edge
pkgMode=lite
script_dir=$PWD
unexpected() { echo "unexpected lifecycle operation" >&2; exit 97; }
install_service_on_systemd() { unexpected; }
install_service_on_sysvinit() { unexpected; }
pkill() { unexpected; }
pgrep() { unexpected; }
service() { unexpected; }
dpkg() { unexpected; }
deb_erase() { unexpected; }
ps() { echo taosd; }
sleep() { unexpected; }
is_version_compatible() { return 0; }
for name in install_main_path install_log install_header install_lib install_config \
            install_examples install_bin finished_install_info; do
    eval "$name() { :; }"
done
install_services
install_service taosd
kill_process taosd
updateProduct
# Ordinary installation must still register services.
imageBuild=0
install_service_on_systemd() { echo registered; }
install_services
"""
        result = subprocess.run(["bash", "-c", script], check=True, capture_output=True, text=True)
        self.assertEqual(result.stdout.splitlines(), ["registered", "registered"])

    def test_dockerfile_propagates_install_failure_without_retry(self):
        dockerfile = (HERE / "Dockerfile").read_text()
        selection = dockerfile[dockerfile.index("    installer_help="):dockerfile.index("    # preserve plugins")]
        selection = selection.replace("\\\n", "\n")
        for supports_image_mode in (False, True):
            with self.subTest(image_mode=supports_image_mode), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                installer = root / "install.sh"
                help_text = "--image-build" if supports_image_mode else "-e no"
                installer.write_text(
                    "#!/bin/bash\n"
                    'if [ "$1" = "-h" ]; then echo "' + help_text + '"; exit 0; fi\n'
                    'echo "$*" >> calls\nexit 42\n'
                )
                installer.chmod(0o755)
                result = subprocess.run(["bash", "-ec", selection], cwd=root)
                self.assertEqual(result.returncode, 42)
                self.assertEqual((root / "calls").read_text().splitlines(),
                                 ["--image-build" if supports_image_mode else "-e no"])

    def test_legacy_cleanup_preserves_identity_and_unrelated_values(self):
        fixtures = {
            "taos.cfg": "fqdn build.host\nmonitorFQDN build.host\nfirstEp other:6030\n",
            "taosx.toml": '[serve]\nfqdn = "build.host"\n[monitor]\nfqdn = "build.host"\n',
            "explorer.toml": 'cluster = "http://build.host:6041"\npassword = "build.host"\nfqdn = "build.host"\n',
            "taosadapter.toml": '[node_exporter]\nurls = ["http://build.host:9100", "http://build.host.other:9100"]\n',
            "taoskeeper.toml": 'host = "build.host"\n[tdengine]\nhost = "build.host"\npassword = "build.host"\n',
        }
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name, content in fixtures.items():
                (root / name).write_text(content)
            command = ["bash", str(HERE / "normalize-legacy-config.sh"), tmp, "build.host"]
            subprocess.run(command, check=True)
            expected = dict(fixtures)
            expected["taos.cfg"] = "monitorFQDN localhost\nfirstEp other:6030\n"
            expected["taosx.toml"] = fixtures["taosx.toml"].replace('[monitor]\nfqdn = "build.host"', '[monitor]\nfqdn = "localhost"')
            expected["explorer.toml"] = fixtures["explorer.toml"].replace("http://build.host:6041", "http://localhost:6041")
            expected["taosadapter.toml"] = fixtures["taosadapter.toml"].replace("http://build.host:9100", "http://localhost:9100")
            expected["taoskeeper.toml"] = fixtures["taoskeeper.toml"].replace('[tdengine]\nhost = "build.host"', '[tdengine]\nhost = "localhost"')
            for _ in range(2):
                for name, content in expected.items():
                    self.assertEqual((root / name).read_text(), content, name)
                subprocess.run(command, check=True)


if __name__ == "__main__":
    unittest.main()
