# -*- coding: utf-8 -*-
from new_test_framework.utils import tdLog, tdSql, etool
import subprocess


class TestFunctionNoFromAll:
    updatecfgDict = {
        "keepColumnName": "1",
        "ttlChangeOnWrite": "1",
        "querySmaOptimize": "1",
        "slowLogScope": "none",
    }

    CASES = [
        ("_qduration", "select _qduration;"),
        ("_qend", "select _qend;"),
        ("_qstart", "select _qstart;"),
        ("abs", "select abs(1);"),
        ("acos", "select acos(1);"),
        ("aes_decrypt", "select aes_decrypt(aes_encrypt('abc', 'key123456789012'), 'key123456789012');"),
        ("aes_encrypt", "select aes_encrypt('abc', 'key123456789012');"),
        ("apercentile", "select apercentile(1, 50);"),
        ("ascii", "select ascii('a');"),
        ("asin", "select asin(1);"),
        ("atan", "select atan(1);"),
        ("avg", "select avg(1);"),
        ("bottom", "select bottom(1, 1);"),
        ("cast", "select cast(1 as int);"),
        ("ceil", "select ceil(1);"),
        ("char", "select char(65);"),
        ("char_length", "select char_length('abc');"),
        ("client_version", "select client_version();"),
        ("concat", "select concat('a', 'b');"),
        ("concat_ws", "select concat_ws(',', 'a', 'b');"),
        ("count", "select count(1);"),
        ("corr", "select corr(1, 2);"),
        ("cos", "select cos(1);"),
        ("crc32", "select crc32('abc');"),
        ("csum", "select csum(1);"),
        ("current_user", "select current_user();"),
        ("database", "select database();"),
        ("date", "select date('2024-01-01 00:00:00');"),
        ("dayofweek", "select dayofweek('2024-01-01 00:00:00');"),
        ("degrees", "select degrees(1);"),
        ("diff", "select diff(1);"),
        ("exp", "select exp(1);"),
        ("fill_forward", "select fill_forward(1);"),
        ("find_in_set", "select find_in_set('a', 'a,b,c');"),
        ("floor", "select floor(1);"),
        ("from_base64", "select from_base64(to_base64('abc'));"),
        ("greatest", "select greatest(1, 1);"),
        ("histogram", "select histogram(1, 'user_input', '[1,2,3]', 0);"),
        ("hyperloglog", "select hyperloglog(1);"),
        ("irate", "select irate(1);"),
        ("least", "select least(1, 1);"),
        ("leastsquares", "select leastsquares(1, 1, 1);"),
        ("length", "select length('abc');"),
        ("like_in_set", "select like_in_set('a', 'a,b,c');"),
        ("ln", "select ln(1);"),
        ("log", "select log(1);"),
        ("lower", "select lower('A');"),
        ("ltrim", "select ltrim(' a');"),
        ("mask_full", "select mask_full('abc', '***');"),
        ("mask_none", "select mask_none('abc');"),
        ("mavg", "select mavg(1, 1);"),
        ("max", "select max(1);"),
        ("md5", "select md5('abc');"),
        ("min", "select min(1);"),
        ("mod", "select mod(1, 1);"),
        ("now", "select now();"),
        ("percentile", "select percentile(1, 50);"),
        ("pi", "select pi();"),
        ("position", "select position('a' in 'cat');"),
        ("pow", "select pow(1, 1);"),
        ("radians", "select radians(1);"),
        ("rand", "select rand();"),
        ("regexp_in_set", "select regexp_in_set('a', 'a,b,c');"),
        ("repeat", "select repeat('a', 2);"),
        ("replace", "select replace('abc', 'a', 'x');"),
        ("round", "select round(1);"),
        ("rtrim", "select rtrim('a ');"),
        ("sample", "select sample(1, 1);"),
        ("server_status", "select server_status();"),
        ("server_version", "select server_version();"),
        ("sha", "select sha('abc');"),
        ("sha1", "select sha1('abc');"),
        ("sha2", "select sha2('abc', 256);"),
        ("sign", "select sign(1);"),
        ("sin", "select sin(1);"),
        ("sm4_decrypt", "select sm4_decrypt(sm4_encrypt('abc', 'key123456789012'), 'key123456789012');"),
        ("sm4_encrypt", "select sm4_encrypt('abc', 'key123456789012');"),
        ("spread", "select spread(1);"),
        ("sqrt", "select sqrt(1);"),
        ("st_astext", "select st_astext(st_geomfromtext('POINT(1 2)'));"),
        ("st_contains", "select st_contains(st_geomfromtext('LINESTRING(0 0, 1 1)'), st_geomfromtext('POINT(1 1)'));"),
        ("st_containsproperly", "select st_containsproperly(st_geomfromtext('LINESTRING(0 0, 1 1)'), st_geomfromtext('POINT(1 1)'));"),
        ("st_covers", "select st_covers(st_geomfromtext('LINESTRING(0 0, 1 1)'), st_geomfromtext('POINT(1 1)'));"),
        ("st_equals", "select st_equals(st_geomfromtext('POINT(1 2)'), st_geomfromtext('POINT(1 2)'));"),
        ("st_geomfromtext", "select st_geomfromtext('POINT(1 2)');"),
        ("st_intersects", "select st_intersects(st_geomfromtext('POINT(1 2)'), st_geomfromtext('POINT(1 2)'));"),
        ("st_makepoint", "select st_makepoint(1, 2);"),
        ("st_touches", "select st_touches(st_geomfromtext('LINESTRING(0 0, 1 1)'), st_geomfromtext('POINT(1 1)'));"),
        ("statecount", "select statecount(1, 'GT', 0);"),
        ("stateduration", "select stateduration(1, 'GT', 0);"),
        ("std", "select std(1);"),
        ("stddev", "select stddev(1);"),
        ("stddev_pop", "select stddev_pop(1);"),
        ("stddev_samp", "select stddev_samp(1);"),
        ("substr", "select substr('abc', 1, 1);"),
        ("substring", "select substring('abc', 1, 1);"),
        ("substring_index", "select substring_index('a.b.c', '.', 2);"),
        ("sum", "select sum(1);"),
        ("tail", "select tail(1, 1);"),
        ("tan", "select tan(1);"),
        ("timediff", "select timediff('2024-01-02 00:00:00', '2024-01-01 00:00:00');"),
        ("timetruncate", "select timetruncate(now(), 1d);"),
        ("timezone", "select timezone();"),
        ("to_base64", "select to_base64('abc');"),
        ("to_char", "select to_char(now(), 'yyyy-mm-dd hh24:mi:ss');"),
        ("to_iso8601", "select to_iso8601(now());"),
        ("to_timestamp", "select to_timestamp('2024-01-01 00:00:00', 'yyyy-mm-dd hh24:mi:ss');"),
        ("to_unixtimestamp", "select to_unixtimestamp('2024-01-01 00:00:00');"),
        ("today", "select today();"),
        ("top", "select top(1, 1);"),
        ("trim", "select trim(' a ');"),
        ("trunc", "select trunc(1, 1);"),
        ("truncate", "select truncate(1, 1);"),
        ("twa", "select twa(1);"),
        ("upper", "select upper('a');"),
        ("user", "select user();"),
        ("var_pop", "select var_pop(1);"),
        ("var_samp", "select var_samp(1);"),
        ("variance", "select variance(1);"),
        ("week", "select week('2024-01-01 00:00:00');"),
        ("weekday", "select weekday('2024-01-01 00:00:00');"),
        ("weekofyear", "select weekofyear('2024-01-01 00:00:00');"),
    ]

    # Unsupported in query without FROM. These calls must fail.
    NEGATIVE_CASES = [
        # count(*) requires a table to resolve the '*' column; only count(1) is valid without FROM.
        ("count_star", "select count(*);"),
        ("unique", "select unique(1);"),
        ("mode", "select mode(1);"),
        ("derivative", "select derivative(1, '1s', 1);"),
        ("to_json", "select to_json(1);"),
        ("_wstart", "select _wstart;"),
        ("_wend", "select _wend;"),
        ("_wduration", "select _wduration;"),
        ("_irowts", "select _irowts;"),
        ("_isfilled", "select _isfilled;"),
        ("tbname", "select tbname();"),
        ("_table_count", "select _table_count;"),
    ]

    def _run_by_taos_cli(self, sql):
        taos_file = etool.taosFile()
        escaped_sql = sql.replace('"', '\\"')
        cmd = f'"{taos_file}" -s "{escaped_sql}"'
        proc = subprocess.run(cmd, shell=True, text=True, capture_output=True)
        return proc.returncode, (proc.stdout or "") + (proc.stderr or "")

    def _is_sql_failed(self, code, output):
        if code != 0:
            return True

        lower = output.lower()
        err_markers = [
            "db error:",
            "syntax error",
            "invalid column name",
            "not supported",
            "failed",
        ]
        return any(marker in lower for marker in err_markers)

    def test_fun_no_from_all(self):
        """Function no-from: 

        1. Execute one fixed no-from call for each function
        2. Ensure every positive call can be executed successfully
        3. Ensure unsupported no-from functions fail as expected

        Catalog:
            - Function

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-02 wpan Add full no-from function tests

        """
        failed = []
        for name, sql in self.CASES:
            code, output = self._run_by_taos_cli(sql)
            if self._is_sql_failed(code, output):
                tail = output.strip().splitlines()
                detail = tail[-1] if tail else "no output"
                failed.append((name, sql, detail))

        if failed:
            preview = []
            for name, sql, detail in failed[:30]:
                tdLog.error(f"positive failed - {name}: {sql} => {detail}")
                preview.append(f"{name}: {sql} => {detail}")
            tdLog.exit(
                f"no-from function calls failed: {len(failed)}\\n" + "\\n".join(preview)
            )

        unexpected_success = []
        for name, sql in self.NEGATIVE_CASES:
            code, output = self._run_by_taos_cli(sql)
            if not self._is_sql_failed(code, output):
                tail = output.strip().splitlines()
                detail = tail[-1] if tail else "no output"
                unexpected_success.append((name, sql, detail))

        if unexpected_success:
            preview = []
            for name, sql, detail in unexpected_success[:30]:
                tdLog.error(f"negative unexpected success - {name}: {sql} => {detail}")
                preview.append(f"{name}: {sql} => {detail}")
            tdLog.exit(
                "unsupported no-from functions unexpectedly succeeded: "
                f"{len(unexpected_success)}\\n" + "\\n".join(preview)
            )
