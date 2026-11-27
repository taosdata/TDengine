# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use it, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Affero General Public License
# for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import hashlib
import importlib.util
import os
import shutil
import stat
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest import mock

spec = importlib.util.spec_from_file_location("windows_ci", Path(__file__).with_name("run_windows_ci.py"))
ci = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ci)


def fake_perl(directory, osname, version="5.040002"):
    """A stand-in perl.exe: answers every invocation with ``<osname> <version>``."""
    directory.mkdir(parents=True, exist_ok=True)
    if os.name == "nt":
        perl = directory / "perl.bat"
        perl.write_text(f"@echo {osname} {version}\r\n", encoding="ascii")
    else:
        perl = directory / "perl"
        perl.write_text(f'#!/bin/sh\necho "{osname} {version}"\n', encoding="ascii")
        perl.chmod(perl.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return perl


class OpenSslPerlTest(unittest.TestCase):
    def test_returns_the_native_windows_perl_found_on_path(self):
        with tempfile.TemporaryDirectory() as temporary:
            perl = fake_perl(Path(temporary) / "strawberry", "MSWin32")
            environment = {"PATH": str(perl.parent)}
            self.assertEqual(ci.ensure_openssl_perl(environment, Path(temporary) / "tsdb"), perl)

    def test_leaves_path_untouched(self):
        # Prepending the perl directory once put Git's usr\bin, which ships a
        # GNU link.exe, ahead of the MSVC tools and broke every rustc link.
        with tempfile.TemporaryDirectory() as temporary:
            perl = fake_perl(Path(temporary) / "strawberry", "MSWin32")
            path = os.pathsep.join(("C:\\VS\\bin", str(perl.parent), "C:\\Windows"))
            environment = {"PATH": path}
            ci.ensure_openssl_perl(environment, Path(temporary) / "tsdb")
            self.assertEqual(environment, {"PATH": path})

    def test_skips_the_perl_bundled_with_git_for_windows(self):
        # That one is an MSYS/Cygwin perl: it cannot run OpenSSL's Configure
        # for the NMake build, so the search has to go on to the next candidate.
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fake_perl(root / "git-usr-bin", "cygwin", "5.042002")
            native = fake_perl(root / "strawberry", "MSWin32")
            environment = {"PATH": os.pathsep.join((str(root / "git-usr-bin"), str(native.parent)))}
            self.assertEqual(ci.ensure_openssl_perl(environment, Path(temporary) / "tsdb"), native)

    def test_prefers_the_configured_perl_over_one_found_on_path(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fake_perl(root / "on-path", "MSWin32")
            configured = fake_perl(root / "elsewhere", "MSWin32")
            environment = {"PATH": str(root / "on-path")}
            with mock.patch.dict(ci.os.environ, {"WIN_OPENSSL_PERL": str(configured)}):
                self.assertEqual(ci.ensure_openssl_perl(environment, Path(temporary) / "tsdb"), configured)

    def test_provisions_strawberry_perl_when_only_an_msys_perl_is_installed(self):
        # Nothing on a stock runner qualifies, so the portable Strawberry Perl
        # is fetched instead of failing.
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fake_perl(root / "git-usr-bin", "cygwin", "5.042002")
            portable = fake_perl(root / "strawberry-perl" / "perl" / "bin", "MSWin32")
            with mock.patch.object(ci, "install_strawberry_perl", return_value=portable) as install:
                perl = ci.ensure_openssl_perl({"PATH": str(root / "git-usr-bin")}, root / "tsdb")
            self.assertEqual(perl, portable)
            install.assert_called_once_with(root / "tsdb")

    def test_does_not_download_when_a_native_perl_is_already_available(self):
        with tempfile.TemporaryDirectory() as temporary:
            perl = fake_perl(Path(temporary) / "strawberry", "MSWin32")
            with mock.patch.object(ci, "install_strawberry_perl") as install:
                ci.ensure_openssl_perl({"PATH": str(perl.parent)}, Path(temporary) / "tsdb")
            install.assert_not_called()


def write_archive(path, files):
    with zipfile.ZipFile(path, "w") as bundle:
        for name, content in files.items():
            bundle.writestr(name, content)


class StrawberryPerlTest(unittest.TestCase):
    def fake_download(self, remote):
        """Stands in for urllib.request.urlretrieve: copies ``remote`` into the cache."""
        def retrieve(url, destination):
            self.assertTrue(url.endswith(ci.STRAWBERRY_PERL_ARCHIVE), url)
            shutil.copyfile(remote, destination)
        return retrieve

    def test_downloads_the_pinned_archive_and_unpacks_only_the_perl_tree(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            remote = root / "remote.zip"
            write_archive(remote, {"perl/bin/perl.exe": "perl", "c/bin/gcc.exe": "gcc", "README.txt": "readme"})
            digest = hashlib.sha256(remote.read_bytes()).hexdigest()
            cache = root / "cache"
            with mock.patch.dict(ci.os.environ, {"WIN_STRAWBERRY_PERL_CACHE": str(cache)}), \
                 mock.patch.object(ci, "STRAWBERRY_PERL_SHA256", digest), \
                 mock.patch.object(ci.urllib.request, "urlretrieve", side_effect=self.fake_download(remote)) as retrieve:
                perl = ci.install_strawberry_perl(root / "tsdb")
            self.assertEqual(retrieve.call_args.args[0], ci.STRAWBERRY_PERL_URL)
            self.assertEqual(Path(retrieve.call_args.args[1]), cache / ci.STRAWBERRY_PERL_ARCHIVE)
            self.assertEqual(perl, cache / ci.STRAWBERRY_PERL_VERSION / "perl" / "bin" / "perl.exe")
            self.assertTrue(perl.is_file())
            self.assertFalse((cache / ci.STRAWBERRY_PERL_VERSION / "c").exists())

    def test_reuses_an_unpacked_copy(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            perl = root / "cache" / ci.STRAWBERRY_PERL_VERSION / "perl" / "bin" / "perl.exe"
            perl.parent.mkdir(parents=True)
            perl.write_text("perl", encoding="ascii")
            with mock.patch.dict(ci.os.environ, {"WIN_STRAWBERRY_PERL_CACHE": str(root / "cache")}), \
                 mock.patch.object(ci.urllib.request, "urlretrieve") as retrieve:
                self.assertEqual(ci.install_strawberry_perl(root / "tsdb"), perl)
            retrieve.assert_not_called()

    def test_replaces_a_cached_archive_with_the_wrong_digest(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            remote = root / "remote.zip"
            write_archive(remote, {"perl/bin/perl.exe": "perl"})
            cache = root / "cache"
            cache.mkdir()
            (cache / ci.STRAWBERRY_PERL_ARCHIVE).write_text("truncated download", encoding="ascii")
            with mock.patch.dict(ci.os.environ, {"WIN_STRAWBERRY_PERL_CACHE": str(cache)}), \
                 mock.patch.object(ci, "STRAWBERRY_PERL_SHA256", hashlib.sha256(remote.read_bytes()).hexdigest()), \
                 mock.patch.object(ci.urllib.request, "urlretrieve", side_effect=self.fake_download(remote)):
                perl = ci.install_strawberry_perl(root / "tsdb")
            self.assertTrue(perl.is_file())

    def test_rejects_a_download_whose_digest_does_not_match(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            tampered = root / "tampered.zip"
            write_archive(tampered, {"perl/bin/perl.exe": "tampered"})
            expected = root / "expected.zip"
            write_archive(expected, {"perl/bin/perl.exe": "perl"})
            cache = root / "cache"
            with mock.patch.dict(ci.os.environ, {"WIN_STRAWBERRY_PERL_CACHE": str(cache)}), \
                 mock.patch.object(ci, "STRAWBERRY_PERL_SHA256", hashlib.sha256(expected.read_bytes()).hexdigest()), \
                 mock.patch.object(ci.urllib.request, "urlretrieve", side_effect=self.fake_download(tampered)):
                with self.assertRaises(RuntimeError) as raised:
                    ci.install_strawberry_perl(root / "tsdb")
            self.assertIn("SHA256", str(raised.exception))
            self.assertFalse((cache / ci.STRAWBERRY_PERL_ARCHIVE).exists())

    def test_fails_naming_the_url_when_the_download_cannot_be_fetched(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            cache = root / "cache"
            with mock.patch.dict(ci.os.environ, {"WIN_STRAWBERRY_PERL_CACHE": str(cache)}), \
                 mock.patch.object(ci.urllib.request, "urlretrieve", side_effect=OSError("unreachable")):
                with self.assertRaises(RuntimeError) as raised:
                    ci.install_strawberry_perl(root / "tsdb")
            self.assertIn(ci.STRAWBERRY_PERL_URL, str(raised.exception))
            self.assertFalse((cache / ci.STRAWBERRY_PERL_ARCHIVE).exists())


if __name__ == "__main__":
    unittest.main()
