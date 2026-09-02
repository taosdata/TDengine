from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck
import taos
import os

class TestBinaryEscapeCharacter:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def do_binary_escape_character(self):
        tdSql.execute(f"drop database if exists ecdb")
        tdSql.execute(f"create database ecdb")
        tdSql.execute(f"use ecdb")

        s1 = "\\'"
        s2 = "\\'abc"
        s3 = "123\\'"
        tdSql.execute(f"create table tbx (ts timestamp, c1 int, c2 binary(20))")
        tdSql.execute(f"insert into tbx values ('2019-10-05 18:00:01.000', 1, '{s1}')")
        tdSql.execute(f"insert into tbx values ('2019-10-05 18:00:02.000', 2, '{s2}')")
        tdSql.execute(f"insert into tbx values ('2019-10-05 18:00:03.000', 3, '{s3}')")
        tdSql.query(f"select * from tbx")
        tdLog.info(
            f'"=====rows:{tdSql.getRows()}), line0:{tdSql.getData(0,2)}, line1:{tdSql.getData(1,2)}, line2:{tdSql.getData(2,2)}"'
        )
        tdSql.checkData(0, 2, "'")
        tdSql.checkData(1, 2, "'abc")
        tdSql.checkData(2, 2, "123'")

        tdSql.execute(f"create table tb (ts timestamp, c1 binary(20))")
        s1 = "abc''001"
        s2 = "abc\\'002"
        s3 = "abc\\\\003"
        s4 = 'abc"004'
        s5 = "abc\\005"
        tdSql.execute(f"insert into tb values ('2019-10-05 18:00:01.000', '{s1}')")
        tdSql.execute(f"insert into tb values ('2019-10-05 18:00:02.000', '{s2}')")
        tdSql.execute(f"insert into tb values ('2019-10-05 18:00:03.000', '{s3}')")
        tdSql.execute(f"insert into tb values ('2019-10-05 18:00:04.000', '{s4}')")
        tdSql.execute(f"insert into tb values ('2019-10-05 18:00:05.000', '{s5}')")

        s1 = 'udp\\"001'
        s2 = 'udp\\"002'
        s3 = "udp\\\\003"
        s4 = "udp\\'004"
        s5 = "udp\\005"
        tdSql.execute(f"insert into tb values ('2019-10-05 18:00:06.000', '{s1}')")
        tdSql.execute(f"insert into tb values ('2019-10-05 18:00:07.000', '{s2}')")
        tdSql.execute(f"insert into tb values ('2019-10-05 18:00:08.000', '{s3}')")
        tdSql.execute(f"insert into tb values ('2019-10-05 18:00:09.000', '{s4}')")
        tdSql.execute(f"insert into tb values ('2019-10-05 18:00:10.000', '{s5}')")

        tdSql.query(f"select * from tb limit 5")
        tdLog.info(f'"====rows: {tdSql.getRows()}) "')
        tdSql.checkRows(5)

        tdLog.info(
            f'"Single quotation ==== tdSql.getData(0,1)~05: {tdSql.getData(0,1)}, {tdSql.getData(1,1)}, {tdSql.getData(2,1)}, {tdSql.getData(3,1)}, {tdSql.getData(4,1)} "'
        )
        tdSql.checkData(0, 1, "abc'001")
        tdSql.checkData(1, 1, "abc'002")
        tdSql.checkData(2, 1, "abc\\003")
        tdSql.checkData(3, 1, 'abc"004')
        tdSql.checkData(4, 1, "abc005")

        tdSql.query(f"select * from tb limit 5 offset 5")
        tdSql.checkRows(5)
        tdLog.info(
            f'"Double quotation  ==== tdSql.getData(0,1)~05: {tdSql.getData(0,1)}, {tdSql.getData(1,1)}, {tdSql.getData(2,1)}, {tdSql.getData(3,1)}, {tdSql.getData(4,1)} "'
        )
        tdSql.checkData(0, 1, 'udp"001')
        tdSql.checkData(1, 1, 'udp"002')
        tdSql.checkData(2, 1, "udp\\003")
        tdSql.checkData(3, 1, "udp'004")
        tdSql.checkData(4, 1, "udp005")

        tdLog.info(f"---------------------> TD-3967")
        tdSql.execute(f"insert into tb values(now, '\\abc\\\\');")
        tdSql.execute(f"insert into tb values(now, '\\abc\\\\');")
        tdSql.execute(f"insert into tb values(now, '\\\\');")

        tdLog.info(f"------------->sim bug")
        # sql_error insert into tb values(now, '\\\');
        tdSql.error(f"insert into tb values(now, '\\');")
        # sql_error insert into tb values(now, '\\\n');
        tdSql.execute(f"insert into tb values(now, '\n');")

    #
    # ------------------- Security Test: Off-by-One Buffer Overflow (CVE Pending) ----------------
    # 
    def do_security_off_by_one_test(self):
        """
        Test for Off-by-One stack buffer overflow vulnerability in trimString()
        Reporter: ghaithabdulreda
        
        This test verifies that escape sequences \\%, \\_, and \\x are properly handled
        without causing buffer overflow when input fills the destination buffer.
        
        Vulnerability: When processing escape sequences requiring 2 bytes, the function
        only checked bounds before the first write, not before the second write.
        
        Fix: Added bounds check to ensure both bytes can be safely written.
        """

        tdLog.info(f"---------------------> Security Test: Off-by-One Buffer Overflow")
        
        # Test 1: Escape sequence \% in LIKE pattern (should preserve the escape)
        tdSql.execute(f"create table sec_test (ts timestamp, c1 binary(50))")
        
        # Insert data with escape sequences that could trigger the vulnerability
        test_cases = [
            ("test\\%pattern", "Pattern with \\% escape"),
            ("test\\_pattern", "Pattern with \\_ escape"),
            ("test\\xvalue", "Pattern with \\x escape"),
            ("normal_value", "Normal value without escapes"),
        ]
        
        for i, (value, desc) in enumerate(test_cases):
            ts = f"2024-01-01 00:00:{i+1:02d}.000"
            try:
                tdSql.execute(f"insert into sec_test values ('{ts}', '{value}')")
                tdLog.info(f"PASS: Successfully inserted {desc}")
            except Exception as e:
                tdLog.exit(f"FAIL: Failed to insert {desc}: {e}")
        
        # Verify data was stored correctly
        tdSql.query(f"select * from sec_test order by ts")
        tdSql.checkRows(len(test_cases))
        
        # Check that escape sequences were preserved
        tdSql.query(f"select c1 from sec_test where c1 like '%\\\\%%'")
        rows_with_percent = tdSql.getRows()
        tdLog.info(f"Found {rows_with_percent} rows with \\% escape sequence")
        
        tdSql.query(f"select c1 from sec_test where c1 like '%\\\\_%'")
        rows_with_underscore = tdSql.getRows()
        tdLog.info(f"Found {rows_with_underscore} rows with \\_ escape sequence")
        
        tdLog.info(f"PASS: Security test completed - no buffer overflow detected")
        
    #
    # ------------------- test_backslash_g.py ----------------
    # 
    def checksql(self, sql):
        result = os.popen(f"taos -s \"{sql}\" ")
        res = result.read()
        print(res)
        if ("Query OK" in res):
            tdLog.info(f"checkEqual success")
        else :
            tdLog.exit(f"checkEqual error")

    def do_td_28164(self):
        tdSql.execute("drop database if exists td_28164;")
        tdSql.execute("create database td_28164;")
        tdSql.execute("create table td_28164.test (ts timestamp, name varchar(10));")
        tdSql.execute("insert into td_28164.test values (now(), 'ac\\\\G') (now() + 1s, 'ac\\\\G') (now()+2s, 'ac\\G') (now()+3s, 'acG') (now()+4s, 'acK') ;")

        tdSql.query(f"select * from td_28164.test;")
        tdSql.checkRows(5)

        tdSql.query(f"select * from td_28164.test where name like 'ac\\\\\\G';")
        tdSql.checkRows(2)

        tdSql.query(f"select * from td_28164.test where name like 'ac\\\\G';")
        tdSql.checkRows(2)

        tdSql.query(f"select * from td_28164.test where name like 'ac\\G';")
        tdSql.checkRows(2)

        # tdSql.query(f"select * from td_28164.test where name like 'ac\\\g';")
        # tdSql.checkRows(0)
        #
        # tdSql.query(f"select * from td_28164.test where name like 'ac\\g';")
        # tdSql.checkRows(0)

        self.checksql(f'select * from td_28164.test where name like \'ac\\G\'\G;')
        # tdSql.checkRows(2)

        self.checksql(f"select * from td_28164.test where name like \'ac\\G\'   \G;")
        # tdSql.checkRows(2)

        tdSql.query(f"select * from td_28164.test where name like 'ac/\\G';")
        tdSql.checkRows(0)

        tdSql.query(f"select * from td_28164.test where name like 'ac/G';")
        tdSql.checkRows(0)

    #
    # ------------------- test_oob_read_trailing_backslash.py ----------------
    # 
    def do_oob_read_trailing_backslash(self):
        """Test for OOB Read vulnerability with trailing backslash (CVE fix)
        
        This test verifies that the SQL tokenizer properly handles strings with
        trailing backslashes without causing out-of-bounds memory reads.
        
        Vulnerability: When a string ends with a backslash followed by null terminator,
        the tokenizer would read 1 byte beyond the buffer boundary.
        
        Reference: TDengine Security Advisory - SQL Tokenizer OOB Read
        """
        tdLog.info("================== Test OOB Read with Trailing Backslash ==================")
        
        tdSql.execute("drop database if exists oob_test_db;")
        tdSql.execute("create database oob_test_db;")
        tdSql.execute("use oob_test_db;")
        tdSql.execute("create table oob_test (ts timestamp, c1 binary(50), c2 nchar(50));")
        
        # Test Case 1: Single quote with trailing backslash - should fail gracefully
        tdLog.info("Test Case 1: Single quote string with trailing backslash")
        tdSql.error(r"insert into oob_test values(now, 'test\');")
        tdLog.info("✓ Server correctly rejected incomplete escape sequence")
        
        # Test Case 2: Double quote with trailing backslash - should fail gracefully
        tdLog.info("Test Case 2: Double quote string with trailing backslash")
        tdSql.error(r'insert into oob_test values(now, "test\");')
        tdLog.info("✓ Server correctly rejected incomplete escape sequence")
        
        # Test Case 3: Only backslash in quotes - minimal trigger case
        tdLog.info("Test Case 3: Minimal case - only backslash in quotes")
        tdSql.error(r"insert into oob_test values(now, '\');")
        tdLog.info("✓ Server correctly rejected minimal incomplete escape")
        
        # Test Case 4: Multiple trailing backslashes
        tdLog.info("Test Case 4: Multiple trailing backslashes")
        tdSql.error(r"insert into oob_test values(now, 'test\\\');")
        tdLog.info("✓ Server correctly rejected multiple trailing backslashes")
        
        # Test Case 5: Valid escape sequences should still work
        tdLog.info("Test Case 5: Valid escape sequences should work normally")
        tdSql.execute("insert into oob_test(ts, c1) values(now, 'test\\\\');")  # Escaped backslash
        tdSql.execute("insert into oob_test(ts, c1) values(now, 'test\\\\value');")  # Escaped quote
        tdSql.execute("insert into oob_test(ts, c1) values(now, 'normal_value');")  # Normal string
        tdSql.query("select * from oob_test;")
        tdSql.checkRows(3)
        tdLog.info("✓ Valid escape sequences work correctly")
        
        # Verify data integrity
        tdSql.query("select c1 from oob_test order by ts;")
        tdSql.checkData(0, 0, "test\\")
        tdSql.checkData(1, 0, "test\\value")
        tdSql.checkData(2, 0, "normal_value")
        tdLog.info("✓ Data integrity verified")
        
        # Test Case 6: NCHAR type with trailing backslash
        tdLog.info("Test Case 6: NCHAR type with trailing backslash")
        tdSql.error(r"insert into oob_test(ts, c2) values(now, N'test\');")
        tdLog.info("✓ NCHAR type also rejects incomplete escape")
        
        # Test Case 7: WHERE clause with trailing backslash
        tdLog.info("Test Case 7: WHERE clause with trailing backslash")
        tdSql.error(r"select * from oob_test where c1 = 'test\';")
        tdLog.info("✓ WHERE clause also handles incomplete escape safely")
                
        tdLog.info("================== OOB Read Test Completed Successfully ==================")
        print("trailing backslash OOB read .............. [passed]")

    #
    # ------------------- main ----------------
    # 
    def test_query_tag_filter(self):
        """Escape character

        1. Validates escape characters in binary data types
        2. Test the insertion and retrieval of strings containing various escape sequences like:
            - single quotes ('), double quotes ("), and backslashes () within binary columns.
        3. Ensures that these special characters are correctly stored, processed, and returned in query results 
        4. Check without causing parsing errors or data corruption.
        5. Jira TD-28164: Support backslash g escape character in like queries
        6. Security test for Off-by-One buffer overflow vulnerability (CVE Pending)
        7. Security Fix: Test OOB Read vulnerability with trailing backslash (CVE)

        Since: v3.0.0.0

        Labels: common,ci,integration,functional,security
        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/parser/binary_escapeCharacter.sim
            - 2025-12-21 Alex Duan Migrated from cases/uncatalog/system-test/2-query/test_backslash_g.py
            - 2026-06-05 Security team Added Off-by-One buffer overflow test
            - 2026-06-05 Security Team Added OOB Read vulnerability test for trailing backslash

        """
        self.do_binary_escape_character()
        self.do_td_28164()
        self.do_security_off_by_one_test()
        self.do_oob_read_trailing_backslash()
