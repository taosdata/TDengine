# -*- coding: utf-8 -*-

# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

from new_test_framework.utils import tdLog, tdSql, etool

class TestTaosShellHex:

    HEX_OPTION = "-H"

    def checkHexCommon(self, sql, expected):
        """Run a SQL via taos -s and check expected string in output."""
        rlist = self.taos(f'{self.HEX_OPTION} -s "{sql}"')
        self.checkListString(rlist, expected)

    def checkHexToFile(self, sql, expected):
        """Run a SQL via taos -s with >> redirect, check expected string in output file."""
        outfile = "hex_test.csv"
        self.deleteFile(outfile)
        self.taos(f'{self.HEX_OPTION} -s "{sql}>>{outfile}"')
        rlist = self.readFileToList(outfile)
        self.checkListString(rlist, expected)
        self.deleteFile(outfile)

    def checkDefaultOffCompatibility(self):
        """Without -H, non-printable BINARY should not be shown as hex."""
        tdLog.info("check default behavior without --binary-as-hex")

        rlist = self.taos('-s "select char(3)"')
        if any("0x03" in line for line in rlist):
            tdLog.exit("default mode should not display char(3) as 0x03; hex requires -H")
        tdLog.info("default mode remains backward-compatible (hex disabled)")

    def checkNonPrintableHex(self):
        """Non-printable BINARY values should be displayed in hex format."""
        tdLog.info("check non-printable binary displayed as hex")

        # char(0) = 0x00 - NULL byte, non-printable
        self.checkHexCommon("select char(0)", "0x00")

        # char(1) = 0x01 - SOH, non-printable
        self.checkHexCommon("select char(1)", "0x01")

        # char(3) = 0x03 - ETX, non-printable
        self.checkHexCommon("select char(3)", "0x03")

        # char(31) = 0x1F - US, last control char before space
        self.checkHexCommon("select char(31)", "0x1F")

        # char(127) = 0x7F - DEL, non-printable
        self.checkHexCommon("select char(127)", "0x7F")

    def checkPrintableNormal(self):
        """Printable BINARY values should be displayed as normal text, not hex."""
        tdLog.info("check printable binary displayed as normal text")

        # char(32) = space, printable (boundary)
        # space is printable, should NOT show as 0x20
        rlist = self.taos(f'{self.HEX_OPTION} -s "select char(32)"')
        if any("0x20" in line for line in rlist):
            tdLog.exit("char(32) should be displayed as space, not 0x20")
        tdLog.info("char(32) correctly displayed as normal text")

        # char(65) = 'A', printable
        self.checkHexCommon("select char(65)", "A")

        # char(122) = 'z', printable
        self.checkHexCommon("select char(122)", "z")

        # char(126) = '~', last printable ASCII before DEL
        self.checkHexCommon("select char(126)", "~")

    def checkWhitespaceChars(self):
        """Tab, newline, carriage return should be treated as non-printable."""
        tdLog.info("check whitespace chars (tab/newline/cr) displayed as hex")

        tests = [
            (9, "tab", "0x09"),
            (10, "newline", "0x0A"),
            (13, "carriage return", "0x0D"),
        ]

        for char_code, name, hex_str in tests:
            rlist = self.taos(f'{self.HEX_OPTION} -s "select char({char_code})"')
            if not any(hex_str in line for line in rlist):
                tdLog.exit(f"char({char_code}) {name} should be displayed as {hex_str}")
            tdLog.info(f"char({char_code}) {name} correctly displayed as hex")

    def checkMultiByteHex(self):
        """Multiple non-printable bytes should all be shown in hex."""
        tdLog.info("check multi-byte non-printable binary hex display")

        # char(1, 2, 3) = 3 bytes, all non-printable
        self.checkHexCommon("select char(1, 2, 3)", "0x010203")

        # char(0, 255) = 2 bytes, 0x00 and 0xFF
        self.checkHexCommon("select char(0, 255)", "0x00FF")

    def checkMixedPrintableNonPrintable(self):
        """If any byte is non-printable, the entire value shows as hex."""
        tdLog.info("check mixed printable/non-printable shows as hex")

        # char(65, 3) = 'A' + 0x03, mixed -> should show as hex
        self.checkHexCommon("select char(65, 3)", "0x4103")

        # char(3, 65) = 0x03 + 'A', mixed -> should show as hex
        self.checkHexCommon("select char(3, 65)", "0x0341")

    def checkHexFileOutput(self):
        """Non-printable BINARY should also be hex in file dump output."""
        tdLog.info("check hex display in file dump output")

        # non-printable -> hex in file
        self.checkHexToFile("select char(3)", '"0x03"')
        self.checkHexToFile("select char(1, 2, 3)", '"0x010203"')

        # printable -> normal text in file
        self.checkHexToFile("select char(65)", '"A"')

        # space (0x20) is printable, should NOT be rendered as hex in file
        outfile = "hex_test.csv"
        self.deleteFile(outfile)
        self.taos(f'{self.HEX_OPTION} -s "select char(32)>>{outfile}"')
        rlist = self.readFileToList(outfile)
        if any("0x20" in line for line in rlist):
            tdLog.exit("char(32) in file output should be displayed as space, not 0x20")
        tdLog.info("char(32) in file output correctly displayed as normal text")
        self.deleteFile(outfile)

    def checkInvalidMultibyteFallbackHex(self):
        """Invalid multibyte BINARY should fall back to full hex output."""
        tdLog.info("check invalid multibyte binary fallback to hex")

        # single invalid high byte should not be truncated/blank
        self.checkHexCommon("select char(255)", "0xFF")

        # mixed ascii + invalid high byte should render full payload in hex
        self.checkHexCommon("select char(99, 167)", "0x63A7")
        self.checkHexCommon("select char(38, 236, 71)", "0x26EC47")

        # bytes observed as blank in terminal should be forced to hex
        self.checkHexCommon("select char(187)", "0xBB")
        self.checkHexCommon("select char(177)", "0xB1")
        self.checkHexCommon("select char(183)", "0xB7")
        self.checkHexCommon("select char(216)", "0xD8")
        self.checkHexCommon("select char(229)", "0xE5")

        # fallback behavior should be consistent for file dump
        self.checkHexToFile("select char(255)", '"0xFF"')
        self.checkHexToFile("select char(99, 167)", '"0x63A7"')

    def checkBlobType(self):
        """BLOB / MEDIUMBLOB text display and hex fallback, plus GEOMETRY dump.

        Covers the shell rendering touched by this feature:
        1. printable BLOB / MEDIUMBLOB -> shown as text on screen
        2. non-printable BLOB / MEDIUMBLOB -> shown as \\x hex on screen
        3. -H forces hex even for printable BLOB
        4. the same rules apply to >> file dump (CSV quoting, quote doubling)
        5. GEOMETRY >> file dump emits uppercase hex (buf-vs-tmp and signedness)

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        """
        tdLog.info("check blob / mediumblob text display and geometry dump")

        db = "shell_blob"
        tb = "t"      # one BLOB column (a table allows only one BLOB)
        tmb = "tm"     # one MEDIUMBLOB column
        tbg = "tg"     # GEOMETRY column

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 1")
        tdSql.execute(f"create table {db}.{tb} (ts timestamp, b blob)")
        tdSql.execute(f"create table {db}.{tmb} (ts timestamp, mb mediumblob)")
        tdSql.execute(f"create table {db}.{tbg} (ts timestamp, g geometry(64))")

        # BLOB: printable rows (plain text + embedded double quote) and a
        # non-printable row (\\x hex literal writes raw bytes including 0x00)
        tdSql.execute(
            f"insert into {db}.{tb} values "
            "('2024-01-01 00:00:00.001', 'hello blob') "
            "('2024-01-01 00:00:00.002', 'a\"b') "
            "('2024-01-01 00:00:00.003', '\\xDEAD00')"
        )

        # MEDIUMBLOB: printable + non-printable
        tdSql.execute(
            f"insert into {db}.{tmb} values "
            "('2024-01-01 00:00:00.001', 'hello mediumblob') "
            "('2024-01-01 00:00:00.002', 'plain mediumblob') "
            "('2024-01-01 00:00:00.003', '\\x00FF00')"
        )

        tdSql.execute(
            f"insert into {db}.{tbg} values ('2024-01-01 00:00:00.001', 'POINT(1 1)')"
        )

        # 1. screen: printable BLOB / MEDIUMBLOB -> text
        rlist = self.taos(f'-s "select b from {db}.{tb}"')
        self.checkListString(rlist, "hello blob")
        self.checkListString(rlist, 'a"b')

        rlist = self.taos(f'-s "select mb from {db}.{tmb}"')
        self.checkListString(rlist, "hello mediumblob")
        self.checkListString(rlist, "plain mediumblob")

        # 2. screen: non-printable BLOB / MEDIUMBLOB -> \\x hex
        rlist = self.taos(f'-s "select b from {db}.{tb}"')
        self.checkListString(rlist, "\\xDEAD00")

        rlist = self.taos(f'-s "select mb from {db}.{tmb}"')
        self.checkListString(rlist, "\\x00FF00")

        # 3. screen: -H forces hex even for printable BLOB
        rlist = self.taos(f'{self.HEX_OPTION} -s "select b from {db}.{tb}"')
        if any("hello blob" in line for line in rlist):
            tdLog.exit("-H should force hex for printable BLOB, but text found")
        self.checkListString(rlist, "\\x68656C6C6F20626C6F62")

        # 4. file dump: printable -> CSV quoted text, quote doubled; non-printable -> hex
        outfile = "shell_blob.csv"
        self.deleteFile(outfile)
        self.taos(f'-s "select b from {db}.{tb}>>{outfile}"')
        rlist = self.readFileToList(outfile)
        self.checkListString(rlist, "hello blob")
        self.checkListString(rlist, 'a""b')
        self.checkListString(rlist, "\\xDEAD00")
        self.deleteFile(outfile)

        self.taos(f'-s "select mb from {db}.{tmb}>>{outfile}"')
        rlist = self.readFileToList(outfile)
        self.checkListString(rlist, "hello mediumblob")
        self.checkListString(rlist, "\\x00FF00")
        self.deleteFile(outfile)

        # 5. file dump with -H: printable BLOB forced to hex
        self.taos(f'{self.HEX_OPTION} -s "select b from {db}.{tb}>>{outfile}"')
        rlist = self.readFileToList(outfile)
        if any("hello blob" in line for line in rlist):
            tdLog.exit("-H should force hex in file dump, but text found")
        self.checkListString(rlist, "\\x68656C6C6F20626C6F62")
        self.deleteFile(outfile)

        # 6. geometry >> file dump: the encoded WKB hex (uppercase), not the
        #    previously empty uninitialized buffer; bytes >= 0x80 (0xF0) stay
        #    two digits instead of sign-extending to FFFFFFxx.
        outfile = "shell_geom.csv"
        self.deleteFile(outfile)
        self.taos(f'-s "select g from {db}.{tbg}>>{outfile}"')
        rlist = self.readFileToList(outfile)
        self.checkListString(rlist, "0101000000")  # WKB point magic, little-endian
        self.checkListString(rlist, "F03F")        # high byte 0xF0 rendered correctly
        if any("FFFFF" in line for line in rlist):
            tdLog.exit("geometry dump contains sign-extended bytes (signedness bug)")
        self.deleteFile(outfile)

        # 7. screen geometry still shows WKT text
        rlist = self.taos(f'-s "select g from {db}.{tbg}"')
        self.checkListString(rlist, "POINT")

        tdSql.execute(f"drop database if exists {db}")

    # run
    def test_tool_taos_shell_hex(self):
        """taos shell hex display for non-printable BINARY

        Test that the taos shell displays non-printable BINARY data in
        hexadecimal format (e.g., 0x03) instead of raw invisible bytes.
        Printable characters should still be displayed as normal text.

        1. Check non-printable chars (0x00-0x1F, 0x7F) show as hex
        2. Check printable chars (0x20-0x7E) show as normal text
        3. Check whitespace chars (tab/newline/cr) are treated as non-printable
        4. Check multi-byte non-printable values show as hex
        5. Check mixed printable/non-printable shows entire value as hex
        6. Check hex display works in file dump output
        7. Check invalid multibyte bytes fallback to full hex

        Since: v3.4.1.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-04-02 Added hex display test for non-printable BINARY

        """
        tdLog.debug(f"start to execute {__file__}")

        # default mode (without -H)
        self.checkDefaultOffCompatibility()

        # non-printable chars -> hex
        self.checkNonPrintableHex()
        # printable chars -> normal text
        self.checkPrintableNormal()
        # whitespace chars -> hex
        self.checkWhitespaceChars()
        # multi-byte hex
        self.checkMultiByteHex()
        # mixed printable/non-printable
        self.checkMixedPrintableNonPrintable()
        # file dump output
        self.checkHexFileOutput()
        # invalid multibyte fallback
        self.checkInvalidMultibyteFallbackHex()
        # blob and medium blob show
        self.checkBlobType()
