# -*- coding: utf-8 -*-
import os
import re
import shutil
import subprocess
import tempfile

from new_test_framework.utils import tdLog, tdSql, tdCom, tdDnodes


class TestQueryClientCharset:
    """Per-query client charset pass-down for regular queries.

    The server runs one charset (UTF-8); a client declares its own charset per
    connection. NCHAR is stored as charset-independent UCS-4, so a GBK client
    and a UTF-8 client MUST get identical results for every NCHAR predicate
    over the same data — proving the querying client's charset (not the
    server's) drives NCHAR<->MBS conversion at every site: literal compare,
    range/BETWEEN, LIKE/MATCH, string funcs, CAST, CASE WHEN (searched &
    simple), tag-index equality & range probe, const folding, and regex
    functions that intentionally use the executing process charset.

    The cross-charset assertion is `gbk == utf8 == expected`: any site that fell
    back to the server charset would diverge for one client.

    Queries run through the `taos` CLI, NOT taospy: taospy's taos_query()
    always encodes the SQL as UTF-8 regardless of the connection charset, so it
    cannot represent a real non-UTF-8 client. The CLI instead reads a -f file
    encoded in the client's charset, i.e. sends the literal bytes in that
    charset, which is exactly what a genuine GBK client does.

    VARCHAR stores the writer's raw bytes, so VARCHAR/JSON cases are asserted
    only for a client whose charset matches the write charset (UTF-8, via
    tdSql here).
    """

    updatecfgDict = {"charset": "UTF-8"}

    def teardown_method(self, method):
        workdir = getattr(self, "workdir", None)
        if workdir:
            shutil.rmtree(workdir, ignore_errors=True)

    def _setup_cli(self):
        build = tdCom.getBuildPath()
        self.taos = os.path.join(build, "build", "bin", "taos")
        self.workdir = tempfile.mkdtemp(prefix="charset_cli_")
        srccfg = os.path.join(tdDnodes.dnodes[0].cfgDir, "taos.cfg")
        with open(srccfg) as f:
            base = [
                ln for ln in f
                if not ln.strip().lower().startswith(("charset", "locale"))
            ]
        self.cfg = {}
        for cs in ("UTF-8", "GBK"):
            d = os.path.join(self.workdir, f"cfg_{cs}")
            os.makedirs(d, exist_ok=True)
            with open(os.path.join(d, "taos.cfg"), "w") as f:
                f.writelines(base)
                f.write(f"charset {cs}\n")
            self.cfg[cs] = d

    def _count(self, charset, sql):
        # Send the SQL to the taos CLI as a -f file encoded in `charset`, so
        # the literal bytes are in that charset (the connection charset the
        # vnode sees per query). count(*) output is ASCII, so parsing is
        # charset-independent.
        out = self._run_cli(charset, sql)
        m = re.search(r"^\s+(\d+)\s+\|", out, re.M)
        if not m:
            tdLog.exit(
                f"count not parsed charset={charset} sql={sql} output={out}"
            )
        return int(m.group(1))

    def _exec_cli(self, charset, sql):
        # taos -f executes line by line, so each statement must be on ONE line;
        # collapse the readability newlines/indentation in the SQL.
        sql = " ".join(sql.split())
        fpath = os.path.join(self.workdir, "q.sql")
        with open(fpath, "wb") as fh:
            fh.write((sql + ";\n").encode(charset))
        proc = subprocess.run(
            [self.taos, "-c", self.cfg[charset], "-f", fpath],
            capture_output=True, timeout=60,
        )
        out = proc.stdout.decode(charset, "replace")
        err = proc.stderr.decode(charset, "replace")
        return proc, out, err

    def _run_cli(self, charset, sql):
        proc, out, err = self._exec_cli(charset, sql)
        if proc.returncode != 0:
            tdLog.exit(
                f"""taos CLI failed charset={charset} rc={proc.returncode}
                sql={sql} stdout={out} stderr={err}"""
            )
        return out

    def _values(self, charset, sql, headers=None):
        headers = set(headers or [])
        out = self._run_cli(charset, sql)
        values = []
        for line in out.splitlines():
            if "|" not in line:
                continue
            stripped = line.strip()
            if not stripped or set(stripped) <= set("=+-|"):
                continue
            parts = line.split("|")
            if len(parts) != 2 or not parts[0].strip() or parts[1].strip():
                tdLog.exit(
                    f"unexpected single-column result row: charset={charset}, sql={sql}, line={line}, output={out}"
                )
            cell = parts[0].strip()
            if cell not in headers:
                values.append(cell)
        if not values:
            tdLog.exit(
                f"""no result values parsed charset={charset} sql={sql}
                output={out}"""
            )
        return values

    def _value(self, charset, sql, headers=None):
        values = self._values(charset, sql, headers)
        if len(values) != 1:
            tdLog.exit(
                f"""expect single value charset={charset} sql={sql}
                values={values}"""
            )
        return values[0]

    def _chk_same(self, name, sql, expect):
        # NCHAR is UCS-4 canonical: both clients must agree AND match expect.
        g = self._count("GBK", sql)
        u = self._count("UTF-8", sql)
        if g == expect and u == expect:
            tdLog.info(f"PASS nchar:{name}: gbk={g} utf8={u}")
        else:
            tdLog.exit(
                f"""FAIL nchar:{name}: expect={expect} gbk={g} utf8={u}
                sql={sql}"""
            )

    def _chk_one(self, name, sql, expect):
        got = self._count("UTF-8", sql)
        if got == expect:
            tdLog.info(f"PASS varchar:{name}: {got}")
        else:
            tdLog.exit(
                f"""FAIL varchar:{name}: expect={expect} got={got}
                sql={sql}"""
            )

    def _chk_value_same(self, name, sql, expect, headers=None):
        g = self._value("GBK", sql, headers)
        u = self._value("UTF-8", sql, headers)
        if g == expect and u == expect:
            tdLog.info(f"PASS nchar-value:{name}: gbk={g} utf8={u}")
        else:
            tdLog.exit(
                f"""FAIL nchar-value:{name}: expect={expect} gbk={g} utf8={u}
                sql={sql}"""
            )

    def _prepare(self):
        tdSql.execute("drop database if exists cst")
        tdSql.execute("create database cst vgroups 2")
        tdSql.execute("use cst")
        tdSql.execute(
            """create table nt (ts timestamp, nc nchar(32), vc varchar(64),
               num int)"""
        )
        tdSql.execute(
            """create table stb (ts timestamp, v int)
               tags (tnc nchar(32), tvc varchar(64))"""
        )
        tdSql.execute("create table jst (ts timestamp, v int) tags (jt json)")
        rows = [
            (1751600000000, "中文测试", "中文测试", 1),
            (1751600001000, "你好世界", "不同内容", 2),
            (1751600002000, "ASCII", "ASCII", 3),
            (1751600003000, "中文abc混合", "中文abc混合", 4),
        ]
        for ts, nc, vc, num in rows:
            tdSql.execute(f"insert into nt values ({ts},'{nc}','{vc}',{num})")
        tdSql.execute("insert into nt values (1751600004000,NULL,NULL,5)")
        tdSql.execute(
            """create table ris (ts timestamp, pat nchar(32), vals nchar(64),
               nsep nchar(4), vsep varchar(4), expected int)"""
        )
        tdSql.execute(
            """insert into ris values
               (1751600010000,'^北.$','北京|上海','|','|',1)
               (1751600011000,'^深.$','北京;深圳',';',';',2)"""
        )
        tdSql.execute(
            """create table ris_broadcast
               (ts timestamp, sep nchar(4), expected int)"""
        )
        tdSql.execute(
            """insert into ris_broadcast values
               (1751600012000,',',2)
               (1751600013000,';',0)"""
        )
        tdSql.execute(
            """insert into sub1 using stb tags ('标签甲','标签甲')
               values (1751600000000,1)"""
        )
        tdSql.execute(
            """insert into sub2 using stb tags ('标签乙','标签乙')
               values (1751600000000,2)"""
        )
        tdSql.execute(
            """insert into sub3 using stb tags ('label_c','label_c')
               values (1751600000000,3)"""
        )
        tdSql.execute(
            """insert into j1 using jst
               tags ('{\"city\":\"北京\",\"tag\":\"值一\"}')
               values (1751600000000,1)"""
        )
        tdSql.execute(
            """insert into j2 using jst
               tags ('{\"city\":\"上海\",\"tag\":\"值二\"}')
               values (1751600000000,2)"""
        )
        # Codepoint-ordered NCHAR dataset for range/BETWEEN/simple-CASE
        # (taosUcs4Compare is codepoint order; equal-prefix shorter sorts
        # first):
        #   abc(0x61) < 世界(世=0x4E16) < 中文(中=0x4E2D) < 中文测试 < 你好(你=0x4F60)
        tdSql.execute("create table ntr (ts timestamp, nc nchar(32), num int)")
        tdSql.execute(
            "create table stbr (ts timestamp, v int) tags (tnc nchar(32))"
        )
        for ts, nc, num in [
            (1751600000000, "世界", 1),
            (1751600001000, "中文", 2),
            (1751600002000, "中文测试", 3),
            (1751600003000, "你好", 4),
            (1751600004000, "abc", 5),
        ]:
            tdSql.execute(f"insert into ntr values ({ts},'{nc}',{num})")
        tdSql.execute("insert into ntr values (1751600005000,NULL,6)")
        tdSql.execute(
            """insert into r1 using stbr tags ('世界')
               values (1751600000000,1)"""
        )
        tdSql.execute(
            """insert into r2 using stbr tags ('中文')
               values (1751600000000,2)"""
        )
        tdSql.execute(
            """insert into r3 using stbr tags ('中文测试')
               values (1751600000000,3)"""
        )
        tdSql.execute(
            """insert into r4 using stbr tags ('你好')
               values (1751600000000,4)"""
        )
        tdSql.execute(
            """insert into r5 using stbr tags ('abc')
               values (1751600000000,5)"""
        )

    def _nchar_cross(self):
        c = self._chk_same
        c("eq",       "select count(*) from cst.nt where nc = '中文测试'", 1)
        c("neq",      "select count(*) from cst.nt where nc <> '中文测试'", 3)
        c(
            "in",
            "select count(*) from cst.nt where nc in ('中文测试','你好世界')",
            2,
        )
        c("like_pre", "select count(*) from cst.nt where nc like '中文%'", 2)
        c("match",    "select count(*) from cst.nt where nc match '中文'", 2)
        # Char-boundary regex exposes the NCHAR-MATCH charset bug. '.' matches
        # one CHARACTER and ^...$ anchors the length, so the result depends on
        # how many characters the server's regex engine (its own locale) sees.
        # NCHAR is UCS-4; converting it to MBS by the *server* charset keeps a
        # 中-family char at its real width, but converting by the *querying
        # client's* charset (the filter.c bug) hands the engine bytes in the
        # wrong encoding. A literal like '中文' can't catch this — both operands
        # mis-encode consistently and still match byte-for-byte; only a
        # '.'-counting / anchored pattern does.
        #
        # match_charlen: pure-ASCII pattern -> encodes identically in every
        #   client charset, so the divergence is the COLUMN conversion alone
        #   (filter.c filterExecuteImpl/Misc). 中文测试/你好世界 are 4 chars.
        c("match_charlen",
          "select count(*) from cst.nt where nc match '^....$'", 2)
        # match_cn_anchor: the pattern itself contains NCHAR (中文), so it also
        #   exercises the PATTERN conversion (fltInitValFieldData). A GBK client
        #   encodes 中文 to 2 bytes/char -> under the UTF-8 server locale the
        #   anchored length no longer matches. Catches the bug even if only the
        #   pattern site were left unfixed. Only 中文测试 is 中文 + exactly 2
        #   more.
        c("match_cn_anchor",
          "select count(*) from cst.nt where nc match '^中文..$'", 1)
        c(
            "nmatch",
            """select count(*) from cst.nt where nc nmatch '中文'
               and nc is not null""",
            2,
        )
        # POSIX regex consumes MBS in the executing process locale. NCHAR
        # operands must therefore use process-charset conversion consistently,
        # independent of the querying client's charset.
        c(
            "regexp_in_set_charlen",
            """select count(*) from cst.nt
               where regexp_in_set('^....$', nc)=1""",
            2,
        )
        c(
            "regexp_in_set_dynamic_nchar_separator",
            """select count(*) from cst.ris
               where regexp_in_set(pat, vals, nsep)=expected""",
            2,
        )
        # The dynamic separator branch previously inverted its type check and
        # tried to decode VARCHAR bytes as UCS-4. This case is independent of
        # the cross-client mismatch but belongs to the same conversion path.
        c(
            "regexp_in_set_dynamic_varchar_separator",
            """select count(*) from cst.ris
               where regexp_in_set(pat, vals, vsep)=expected""",
            2,
        )
        c(
            "regexp_in_set_broadcast_set_dynamic_separator",
            """select count(*) from cst.ris_broadcast
               where regexp_in_set('^b$','a,b',sep)=expected""",
            2,
        )
        c(
            "regexp_extract_nchar_roundtrip",
            """select count(*) from cst.nt where num=1 and
               regexp_extract(nc, '^(.).*$')=substr(nc,1,1)""",
            1,
        )
        c(
            "concat",
            """select count(*) from cst.nt
               where concat(nc,'后缀')='中文测试后缀'""",
            1,
        )
        c(
            "length",
            "select count(*) from cst.nt where char_length(nc)=4 and num=1",
            1,
        )
        c(
            "substr",
            """select count(*) from cst.nt
               where substr(nc,1,2)='中文' and num=1""",
            1,
        )
        c(
            "replace",
            """select count(*) from cst.nt
               where replace(nc,'测试','实验')='中文实验'""",
            1,
        )
        c(
            "casewhen",
            """select count(*) from (select case when nc='中文测试' then '中'
               else 'x' end r from cst.nt) t where r='中'""",
            1,
        )
        c(
            "groupby",
            """select count(*) from (select nc,count(*) c from cst.nt
               where nc is not null group by nc) t""",
            4,
        )
        c(
            "distinct",
            """select count(*) from
               (select distinct nc from cst.nt where nc is not null) t""",
            4,
        )
        c(
            "return_nchar",
            """select count(*) from (select nc r from cst.nt where nc='中文测试')
               t where r='中文测试' and char_length(r)=4""",
            1,
        )
        self._chk_value_same(
            "groupby_order",
            """select group_concat(cast(num as varchar(8)), ',') r from
               (select min(num) num from cst.nt where nc is not null
               group by nc order by nc) t""",
            "3,4,1,2",
            {"r"},
        )
        self._chk_value_same(
            "distinct_order",
            """select group_concat(cast(num as varchar(8)), ',') r from
               (select num from cst.nt where nc in
               (select distinct nc from cst.nt where nc is not null)
               order by nc) t""",
            "3,4,1,2",
            {"r"},
        )
        self._chk_value_same(
            "orderby_first",
            "select num r from cst.nt where nc is not null order by nc limit 1",
            "3",
            {"r"},
        )
        self._chk_value_same(
            "orderby_last",
            """select num r from cst.nt where nc is not null
               order by nc desc limit 1""",
            "2",
            {"r"},
        )
        # NCHAR tag: exercises the tag-index probe path (metaFilterTableIds).
        c("tag_eq",    "select count(*) from cst.stb where tnc='标签甲'", 1)
        c(
            "tag_in",
            "select count(*) from cst.stb where tnc in ('标签甲','标签乙')",
            2,
        )
        c("tag_like",  "select count(*) from cst.stb where tnc like '标签%'", 2)
        c("tag_match", "select count(*) from cst.stb where tnc match '标签'", 2)
        # Constant folding of NCHAR literals (client-side and pushed-down).
        c(
            "cfold",
            """select count(*) from cst.nt
               where cast('中文' as nchar(8)) < cast('你好' as nchar(8))""",
            5,
        )

    def _nchar_range(self):
        # Range/BETWEEN/simple-CASE and NCHAR tag-range over the
        # codepoint-ordered dataset. These exercise runtime literal conversion
        # for comparison operators (< <= > >=, BETWEEN), the simple CASE form
        # (CASE nc WHEN literal — an implicit compare with no OperatorNode) and
        # the tag-index range probe; all must give gbk == utf8 == expect.
        c = self._chk_same
        c("lt",        "select count(*) from cst.ntr where nc < '中文'", 2)
        c("le",        "select count(*) from cst.ntr where nc <= '中文'", 3)
        c("gt",        "select count(*) from cst.ntr where nc > '中文'", 2)
        c("ge",        "select count(*) from cst.ntr where nc >= '中文'", 3)
        c(
            "between",
            "select count(*) from cst.ntr where nc between '世界' and '中文测试'",
            3,
        )
        c(
            "gt_prefix",
            "select count(*) from cst.ntr where nc > '中文' and nc < '你好'",
            1,
        )
        c(
            "scase",
            """select count(*) from
               (select case nc when '中文测试' then 1 else 0 end r from cst.ntr)
               t where r=1""",
            1,
        )
        c(
            "scase_mul",
            """select count(*) from (select case nc when '中文' then 1
               when '你好' then 1 else 0 end r from cst.ntr) t where r=1""",
            2,
        )
        c(
            "scase_nchar_literal",
            """select count(*) from (select case
               cast('你好世界' as nchar(32)) when nc then 1
               else num * 0 end r
               from cst.nt) t where r=1""",
            1,
        )
        c("tag_lt",    "select count(*) from cst.stbr where tnc < '中文'", 2)
        c("tag_gt",    "select count(*) from cst.stbr where tnc > '中文'", 2)
        c("tag_ge",    "select count(*) from cst.stbr where tnc >= '中文'", 3)

    def _varchar_same_charset(self):
        c = self._chk_one
        c(
            "scase_varchar_column_with_nchar_literal",
            """select count(*) from (select case '中文测试'
               when vc then 1 when cast('中文测试' as nchar(32))
               then 2 + num * 0 else 0 end r from cst.nt) t where r=1""",
            1,
        )
        c("vc_eq",    "select count(*) from cst.nt where vc='中文测试'", 1)
        c("nc_eq_vc", "select count(*) from cst.nt where nc=vc", 3)
        c(
            "cast_v2n",
            "select count(*) from cst.nt where cast(vc as nchar(32))='中文测试'",
            1,
        )
        c(
            "cast_n2v",
            """select count(*) from cst.nt
               where cast(nc as varchar(64))=vc and num=1""",
            1,
        )
        c(
            "tag_ncvc",
            "select count(*) from cst.stb where cast(tnc as varchar(64))=tvc",
            3,
        )
        c("json_eq",  "select count(*) from cst.jst where jt->'city'='北京'", 1)
        c(
            "json_mat",
            "select count(*) from cst.jst where jt->'tag' match '值一'",
            1,
        )

    def _pass_down_cross(self):
        # Cross-charset coverage for the DS §5.2 pass-down sites. The
        # count(*) output is ASCII, isolating server-side conversion semantics.
        c = self._chk_same
        # JSON tag inverted-index equality probe by a non-UTF-8 client.
        # The index term is persisted in the server charset; the GBK client must
        # convert its probe constant to match. A raw-byte probe would miss under
        # GBK (gbk=0 != utf8=1).
        c(
            "json_idx_eq_cross",
            "select count(*) from cst.jst where jt->'city'='北京'",
            1,
        )
        # System-table predicates convert NCHAR/JSON tag values to the query
        # charset before comparison. Count output stays ASCII, so both client
        # charsets can exercise the conversion without terminal rendering.
        c(
            "systable_nchar_tag",
            """select count(*) from information_schema.ins_tags
               where db_name='cst' and table_name='sub1' and tag_name='tnc'
               and tag_value='标签甲'""",
            1,
        )
        c(
            "systable_json_tag",
            """select count(*) from information_schema.ins_tags
               where db_name='cst' and table_name='j1' and tag_name='jt'
               and tag_value='{"city":"北京","tag":"值一"}'""",
            1,
        )

    def test_query_client_charset(self):
        """Regular-query results are driven by the per-query client charset.

        1. Every NCHAR predicate returns identical results for a GBK client and
           a UTF-8 client over the same data (eq/neq/in/like/match/nmatch/
           string-funcs/regex/case-when/group/distinct/order/tag-index/const).
        2. Range/BETWEEN/simple-CASE and NCHAR tag-range over an ordered
           dataset.
        3. NCHAR<->VARCHAR mixed comparison and JSON tag for the write charset.
        4. System-table tag rendering in the query charset.

        Since: v3.4.3.0

        Labels: common,ci,functional

        Catalog:
            - Query

        Jira: None

        History:
            - 2026-08-05: Add system-table charset error coverage.
            - 2026-07-21 Add process-charset regex conversion checks
            - 2026-07-07 Add cross-client NCHAR value checks
        """
        self._prepare()
        self._setup_cli()
        if not os.path.isfile(self.taos):
            tdLog.exit(f"taos CLI not found at {self.taos}")

        self._nchar_cross()
        self._nchar_range()
        self._varchar_same_charset()
        self._pass_down_cross()

        tdLog.success(f"{__file__} passed")
