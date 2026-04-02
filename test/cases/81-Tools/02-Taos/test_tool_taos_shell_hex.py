# -*- coding: utf-8 -*-

from new_test_framework.utils import tdLog, tdSql, etool
import os
import platform


class TestTaosShellHex:

    def checkHexCommon(self, sql, expected):
        """Run a SQL via taos -s and check expected string in output."""
        rlist = self.taos(f'-s "{sql}"')
        self.checkListString(rlist, expected)

    def checkHexToFile(self, sql, expected):
        """Run a SQL via taos -s with >> redirect, check expected string in output file."""
        outfile = "hex_test.csv"
        self.deleteFile(outfile)
        self.taos(f'-s "{sql}>>{outfile}"')
        rlist = self.readFileToList(outfile)
        self.checkListString(rlist, expected)
        self.deleteFile(outfile)

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
        rlist = self.taos(f'-s "select char(32)"')
        found_hex = False
        for line in rlist:
            if "0x20" in line:
                found_hex = True
                break
        if found_hex:
            tdLog.exit("char(32) should be displayed as space, not 0x20")
        tdLog.info("char(32) correctly displayed as normal text")

        # char(65) = 'A', printable
        self.checkHexCommon("select char(65)", "A")

        # char(122) = 'z', printable
        self.checkHexCommon("select char(122)", "z")

        # char(126) = '~', last printable ASCII before DEL
        self.checkHexCommon("select char(126)", "~")

    def checkWhitespaceChars(self):
        """Tab, newline, carriage return are excluded from non-printable detection."""
        tdLog.info("check whitespace chars (tab/newline/cr) treated as printable")

        # char(9) = tab - should NOT show as 0x09
        rlist = self.taos(f'-s "select char(9)"')
        found_hex = False
        for line in rlist:
            if "0x09" in line:
                found_hex = True
                break
        if found_hex:
            tdLog.exit("char(9) tab should not be displayed as 0x09")
        tdLog.info("char(9) tab correctly not displayed as hex")

        # char(10) = newline - should NOT show as 0x0A
        rlist = self.taos(f'-s "select char(10)"')
        found_hex = False
        for line in rlist:
            if "0x0A" in line:
                found_hex = True
                break
        if found_hex:
            tdLog.exit("char(10) newline should not be displayed as 0x0A")
        tdLog.info("char(10) newline correctly not displayed as hex")

        # char(13) = carriage return - should NOT show as 0x0D
        rlist = self.taos(f'-s "select char(13)"')
        found_hex = False
        for line in rlist:
            if "0x0D" in line:
                found_hex = True
                break
        if found_hex:
            tdLog.exit("char(13) carriage return should not be displayed as 0x0D")
        tdLog.info("char(13) carriage return correctly not displayed as hex")

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

    def checkInvalidMultibyteFallbackHex(self):
        """Invalid multibyte BINARY should fall back to full hex output."""
        tdLog.info("check invalid multibyte binary fallback to hex")

        # single invalid high byte should not be truncated/blank
        self.checkHexCommon("select char(255)", "0xFF")

        # mixed ascii + invalid high byte should render full payload in hex
        self.checkHexCommon("select char(99, 167)", "0x63A7")
        self.checkHexCommon("select char(38, 236, 71)", "0x26EC47")

        # fallback behavior should be consistent for file dump
        self.checkHexToFile("select char(255)", '"0xFF"')
        self.checkHexToFile("select char(99, 167)", '"0x63A7"')

    # run
    def test_tool_taos_shell_hex(self):
        """taos shell hex display for non-printable BINARY

        Test that the taos shell displays non-printable BINARY data in
        hexadecimal format (e.g., 0x03) instead of raw invisible bytes.
        Printable characters should still be displayed as normal text.

        1. Check non-printable chars (0x00-0x1F, 0x7F) show as hex
        2. Check printable chars (0x20-0x7E) show as normal text
        3. Check whitespace chars (tab/newline/cr) are not treated as non-printable
        4. Check multi-byte non-printable values show as hex
        5. Check mixed printable/non-printable shows entire value as hex
        6. Check hex display works in file dump output
        7. Check invalid multibyte bytes fallback to full hex

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-02 Added hex display test for non-printable BINARY

        """
        tdLog.debug(f"start to execute {__file__}")

        # non-printable chars -> hex
        self.checkNonPrintableHex()
        # printable chars -> normal text
        self.checkPrintableNormal()
        # whitespace chars excluded from non-printable
        self.checkWhitespaceChars()
        # multi-byte hex
        self.checkMultiByteHex()
        # mixed printable/non-printable
        self.checkMixedPrintableNonPrintable()
        # file dump output
        self.checkHexFileOutput()
        # invalid multibyte fallback
        self.checkInvalidMultibyteFallbackHex()
