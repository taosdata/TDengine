###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

from new_test_framework.utils import tdLog, tdSql, etool, tdStream
import os
import shutil
import tempfile
import time


class TestTaosbackupContent:
    """Content selection (-M/--content) and the two-stage restore ordering.

    A virtual table may map each of its columns to an arbitrary db.table.column,
    so its DDL can reference a database other than its own.  The restore must
    therefore create every database's physical tables *before* applying any
    virtual table / stream / topic DDL, otherwise the referenced database may
    still be missing.

    The scenario below is built so that the database holding the virtual tables
    (DB_V) sorts and is listed *before* the database it references (DB_P).
    Passing "-D DB_V,DB_P" forces the failing order on purpose.
    """

    DB_P = "ctdb_phy"   # physical tables — referenced by the virtual tables
    DB_V = "ctdb_vir"   # virtual tables  — reference DB_P
    TOPIC = "ctdb_tp1"
    STREAM = "ctdb_stm1"

    #
    # ------------------- helpers ----------------
    #

    def dump(self, args, expectFail=False):
        """Run taosdump and assert its exit code."""
        cmd = f"{etool.taosDumpFile()} {args}"
        tdLog.info(cmd)
        code = os.system(cmd)
        if expectFail and code == 0:
            tdLog.exit(f"taosdump was expected to fail but succeeded: {cmd}")
        if not expectFail and code != 0:
            tdLog.exit(f"taosdump failed (code={code}): {cmd}")
        return code

    def cleanDbs(self):
        # Topics hold a reference to the subscribed table, so drop them first.
        # Streams are dropped along with their owning database.
        tdSql.execute(f"drop topic if exists {self.TOPIC}")
        tdSql.execute(f"drop database if exists {self.DB_V}")
        tdSql.execute(f"drop database if exists {self.DB_P}")

    def prepareData(self):
        """Create DB_P (physical) and DB_V (virtual, referencing DB_P)."""
        self.cleanDbs()
        tdSql.execute(f"create database {self.DB_P}")
        tdSql.execute(f"create database {self.DB_V}")

        # physical: one super table with 2 child tables, plus a normal table
        tdSql.execute(
            f"create table {self.DB_P}.st (ts timestamp, c1 int, c2 float) "
            f"tags(t1 int, t2 binary(16))"
        )
        tdSql.execute(f"create table {self.DB_P}.d0 using {self.DB_P}.st tags(1, 'aa')")
        tdSql.execute(f"create table {self.DB_P}.d1 using {self.DB_P}.st tags(2, 'bb')")
        tdSql.execute(
            f"insert into {self.DB_P}.d0 values"
            f"(1700000000000, 1, 1.5)(1700000001000, 2, 2.5)"
        )
        tdSql.execute(
            f"insert into {self.DB_P}.d1 values"
            f"(1700000000000, 3, 3.5)(1700000001000, 4, 4.5)"
        )
        tdSql.execute(f"create table {self.DB_P}.ntb (ts timestamp, c1 int)")
        tdSql.execute(f"insert into {self.DB_P}.ntb values(1700000000000, 7)")

        # virtual normal table in DB_V, columns sourced from DB_P
        tdSql.execute(
            f"create vtable {self.DB_V}.vn (ts timestamp, "
            f"c1 int from {self.DB_P}.d0.c1, c2 float from {self.DB_P}.d0.c2)"
        )

        # virtual super table + virtual child tables in DB_V, sourced from DB_P
        tdSql.execute(
            f"create stable {self.DB_V}.vst (ts timestamp, c1 int, c2 float) "
            f"tags(vt1 int) virtual 1"
        )
        tdSql.execute(
            f"create vtable {self.DB_V}.vc0 "
            f"(c1 from {self.DB_P}.d0.c1, c2 from {self.DB_P}.d0.c2) "
            f"using {self.DB_V}.vst tags(10)"
        )
        tdSql.execute(
            f"create vtable {self.DB_V}.vc1 "
            f"(c1 from {self.DB_P}.d1.c1, c2 from {self.DB_P}.d1.c2) "
            f"using {self.DB_V}.vst tags(20)"
        )

        # topic on the physical database
        tdSql.execute(f"create topic {self.TOPIC} as select * from {self.DB_P}.st")

        # stream: source in DB_P, target in DB_V — a cross-db reference just
        # like the virtual tables above, so -W must rewrite it the same way
        tdStream.ensureSnode()
        tdSql.execute(
            f"create stream {self.DB_P}.{self.STREAM} interval(1m) sliding(1m) "
            f"from {self.DB_P}.st into {self.DB_V}.stm_out "
            f"as select _twstart, count(*) from %%trows"
        )

        # sanity check the source before backing it up
        tdSql.query(f"select * from {self.DB_V}.vn")
        tdSql.checkRows(2)
        tdSql.query(f"select * from {self.DB_V}.vc1")
        tdSql.checkRows(2)
        tdSql.query(
            f"select stream_name from information_schema.ins_streams "
            f"where db_name='{self.DB_P}'"
        )
        tdSql.checkRows(1)

    def checkBasicRestored(self):
        """Physical tables and their data must be present."""
        tdSql.query(f"select count(*) from {self.DB_P}.st")
        tdSql.checkData(0, 0, 4)
        tdSql.query(f"select count(*) from {self.DB_P}.ntb")
        tdSql.checkData(0, 0, 1)
        # the virtual super table is a schema object stored in stb.sql, so it is
        # part of the basic content even though its child tables are not
        tdSql.query(f"show {self.DB_V}.stables")
        tdSql.checkRows(1)

    def checkExtMetaRestored(self):
        """Virtual tables, their tags, and the topic must be present."""
        # 3 virtual tables: vn (normal) + vc0/vc1 (children of vst)
        tdSql.query(f"show {self.DB_V}.vtables")
        tdSql.checkRows(3)

        # virtual normal table reads through to DB_P
        tdSql.query(f"select * from {self.DB_V}.vn")
        tdSql.checkRows(2)

        # virtual child tables read through to DB_P
        tdSql.query(f"select * from {self.DB_V}.vc0 order by ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 1)
        tdSql.query(f"select * from {self.DB_V}.vc1 order by ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 3)

        # virtual child tags came back from vtags/.  ins_tags is the same source
        # the backup reads from, and unlike a tag query on the virtual super
        # table it does not depend on the referenced tables returning rows.
        tdSql.query(
            f"select table_name, tag_value from information_schema.ins_tags "
            f"where db_name='{self.DB_V}' and stable_name='vst' "
            f"and tag_name='vt1' order by table_name"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "vc0")
        tdSql.checkData(0, 1, "10")
        tdSql.checkData(1, 0, "vc1")
        tdSql.checkData(1, 1, "20")

        # topic
        tdSql.query("show topics")
        names = [row[0] for row in tdSql.queryResult]
        assert self.TOPIC in names, f"topic {self.TOPIC} not restored, got {names}"

        # stream: source db is DB_P, but wait_streams_ready() polls because
        # stream deployment to the snode happens asynchronously after CREATE STREAM
        self.waitStreamNames(self.DB_P, [self.STREAM])

    def checkExtMetaAbsent(self):
        """No virtual child tables, no topic, and no stream yet."""
        tdSql.query(f"show {self.DB_V}.vtables")
        tdSql.checkRows(0)
        tdSql.query("show topics")
        names = [row[0] for row in tdSql.queryResult]
        assert self.TOPIC not in names, \
            f"topic {self.TOPIC} should not exist after a basic-only restore"
        tdSql.query(
            f"select stream_name from information_schema.ins_streams "
            f"where db_name='{self.DB_P}'"
        )
        tdSql.checkRows(0)

    def waitStreamNames(self, db, expectedNames, timeout=30):
        """Poll ins_streams until expectedNames all appear in db (stream
        deployment to the snode is asynchronous), then assert an exact match."""
        deadline = time.time() + timeout
        names = []
        while time.time() < deadline:
            tdSql.query(
                f"select stream_name from information_schema.ins_streams "
                f"where db_name='{db}'"
            )
            names = [row[0] for row in tdSql.queryResult]
            if set(expectedNames).issubset(set(names)):
                return
            time.sleep(1)
        tdLog.exit(
            f"timed out waiting for streams {expectedNames} in {db}, got {names}"
        )

    #
    # ------------------- sub-cases ----------------
    #

    def do_reverse_order_restore(self, tmpdir):
        """Core regression: restore DB_V before DB_P and expect success.

        Before the two-stage split this failed with "Database not exist" while
        executing DB_V's CREATE VTABLE, and aborted the whole restore so DB_P
        was never restored either.
        """
        backdir = os.path.join(tmpdir, "all")
        self.dump(f"--content=all -D {self.DB_P},{self.DB_V} -o {backdir}")

        # the backup must actually contain the cross-database reference,
        # otherwise this test would pass for the wrong reason
        vtbSql = os.path.join(backdir, self.DB_V, "vtb.sql")
        assert os.path.exists(vtbSql), f"vtb.sql missing: {vtbSql}"
        with open(vtbSql, "r") as f:
            vtbContent = f.read()
        assert self.DB_P in vtbContent, \
            f"vtb.sql has no cross-db reference to {self.DB_P}: {vtbContent!r}"

        self.cleanDbs()
        # -D lists the virtual-table database FIRST on purpose
        self.dump(f"--content=all -D {self.DB_V},{self.DB_P} -i {backdir}")

        self.checkBasicRestored()
        self.checkExtMetaRestored()
        tdLog.info("reverse-order cross-db restore ........ [passed]")

    def do_default_is_basic(self, tmpdir):
        """No --content means basic: no vtb.sql / stream.sql / topic.sql DDL
        files are written. vtags/ is the exception — a virtual STB's child
        tags must still be backed up here, or they would be silently lost
        for anyone who never runs an ext-meta pass."""
        backdir = os.path.join(tmpdir, "default")
        self.dump(f"-D {self.DB_P},{self.DB_V} -o {backdir}")

        for db in (self.DB_P, self.DB_V):
            for name in ("vtb.sql", "stream.sql", "topic.sql"):
                path = os.path.join(backdir, db, name)
                assert not os.path.exists(path), \
                    f"{name} must not be produced by a basic backup: {path}"
            # db.sql is basic content and must be there
            assert os.path.exists(os.path.join(backdir, db, "db.sql")), \
                f"db.sql missing for {db}"

        # DB_P has no virtual STB, so it has nothing to put in vtags/
        vtagsP = os.path.join(backdir, self.DB_P, "vtags")
        assert not os.path.exists(vtagsP), \
            f"vtags/ unexpected for a db with no virtual STB: {vtagsP}"

        # DB_V owns virtual STB vst: its child tags must be backed up even by
        # a basic-only pass, since nothing else guarantees an ext-meta pass runs
        vtagsV = os.path.join(backdir, self.DB_V, "vtags")
        assert os.path.exists(vtagsV), \
            f"vtags/ for virtual STB vst missing from basic backup: {vtagsV}"

        tdLog.info("default content is basic ............... [passed]")

    def do_split_restore(self, tmpdir):
        """basic pass + ext-meta pass == a single all pass."""
        backdir = os.path.join(tmpdir, "all")   # reuse the --content=all backup

        self.cleanDbs()
        self.dump(f"--content=basic -D {self.DB_V},{self.DB_P} -i {backdir}")
        self.checkBasicRestored()
        self.checkExtMetaAbsent()

        self.dump(f"--content=ext-meta -D {self.DB_V},{self.DB_P} -i {backdir}")
        self.checkBasicRestored()
        self.checkExtMetaRestored()
        tdLog.info("basic + ext-meta == all ................. [passed]")

    def do_extmeta_only_creates_db(self, tmpdir):
        """--content=ext-meta re-creates a missing database before applying DDL."""
        backdir = os.path.join(tmpdir, "all")

        # drop only the virtual-table database; DB_P stays so the references resolve
        tdSql.execute(f"drop database if exists {self.DB_V}")
        self.dump(f"--content=ext-meta -D {self.DB_V} -i {backdir}")

        tdSql.query(f"show {self.DB_V}.vtables")
        tdSql.checkRows(3)
        tdSql.query(f"select * from {self.DB_V}.vn")
        tdSql.checkRows(2)
        tdLog.info("ext-meta-only recreates database ........ [passed]")

    def do_extmeta_only_backup(self, tmpdir):
        """An ext-meta-only backup still writes db.sql so restore can discover it."""
        backdir = os.path.join(tmpdir, "extonly")
        self.dump(f"--content=ext-meta -D {self.DB_P},{self.DB_V} -o {backdir}")

        # db.sql is required both for database discovery (no -D) and for
        # creating the database in the extended-metadata stage
        for db in (self.DB_P, self.DB_V):
            assert os.path.exists(os.path.join(backdir, db, "db.sql")), \
                f"ext-meta-only backup must write db.sql for {db}"
        assert os.path.exists(os.path.join(backdir, self.DB_V, "vtb.sql")), \
            "ext-meta-only backup must write vtb.sql"
        assert os.path.exists(os.path.join(backdir, self.DB_P, "topic.sql")), \
            "ext-meta-only backup must write topic.sql"
        assert os.path.exists(os.path.join(backdir, self.DB_P, "stream.sql")), \
            "ext-meta-only backup must write stream.sql"
        # no time-series data is exported in this mode
        assert not os.path.exists(os.path.join(backdir, self.DB_P, "st_data0")), \
            "ext-meta-only backup must not export data files"

        # restore without -D: databases are discovered by scanning for db.sql
        tdSql.execute(f"drop topic if exists {self.TOPIC}")
        tdSql.execute(f"drop stream if exists {self.STREAM}")
        tdSql.execute(f"drop database if exists {self.DB_V}")
        self.dump(f"--content=ext-meta -i {backdir}")
        self.checkExtMetaRestored()

        # Deterministic ordering guard: restore again with an explicit -D list
        # that puts the referencing database (DB_P, whose stream targets DB_V)
        # FIRST.  This is the exact order that previously failed with
        # "Database not exist" — the two-pass split must create every database
        # (Prepare) before applying any DDL (Apply), so it must succeed no
        # matter which order raw readdir() (used by -i discovery) produces.
        tdSql.execute(f"drop topic if exists {self.TOPIC}")
        tdSql.execute(f"drop stream if exists {self.STREAM}")
        tdSql.execute(f"drop database if exists {self.DB_V}")
        self.dump(f"--content=ext-meta -D {self.DB_P},{self.DB_V} -i {backdir}")
        self.checkExtMetaRestored()
        tdLog.info("ext-meta-only backup and restore ........ [passed]")

    def do_invalid_content(self, tmpdir):
        """An unknown --content value is rejected."""
        backdir = os.path.join(tmpdir, "bogus")
        self.dump(f"--content=bogus -D {self.DB_P} -o {backdir}", expectFail=True)
        self.dump(f"-M nosuchvalue -D {self.DB_P} -o {backdir}", expectFail=True)
        tdLog.info("invalid content rejected ............... [passed]")

    def streamDdl(self):
        """Return the current CREATE STREAM text for self.STREAM (post-restore)."""
        tdSql.query(
            f"select sql from information_schema.ins_streams "
            f"where stream_name='{self.STREAM}'"
        )
        tdSql.checkRows(1)
        return tdSql.queryResult[0][0]

    def vtableDdl(self, dbName):
        """Return the current SHOW CREATE VTABLE text for {dbName}.vn."""
        tdSql.query(f"show create vtable {dbName}.vn")
        tdSql.checkRows(1)
        return tdSql.queryResult[0][1]

    def restoreWithRename(self, backdir, renameMap):
        """Restore --content=all with -W built from {oldDb: newDb}, dropping targets first."""
        for newDb in renameMap.values():
            tdSql.execute(f"drop database if exists {newDb}")
        pairs = "|".join(f"{old}->{new}" for old, new in renameMap.items())
        self.dump(f'--content=all -W "{pairs}" -D {self.DB_V},{self.DB_P} -i {backdir}')

    def do_rename_referencing_db_only(self, tmpdir):
        """-W renames only DB_V — the database the virtual table LIVES IN, and
        the stream's cross-db TARGET. DB_P — the database the virtual table
        READS FROM, the stream's own db, and the topic's only db — is left
        untouched.

        The virtual table case is the ordinary single-database rename and
        already worked before the -W fix: DB_V's own DDL is always qualified
        with its target name. The stream case is not symmetric with the other
        two sub-cases though: DB_V here is "someone else's database" from the
        stream's point of view (the stream's own db is DB_P), so rewriting the
        stream's target still needs applyAllRenamesInSql() to find DB_V in the
        -W map even though the stream itself is being restored under DB_P.
        The topic, which only ever references DB_P, must be completely
        unaffected.
        """
        backdir = os.path.join(tmpdir, "all")   # reuse the --content=all backup
        newV = "zz_renamed_v"

        self.cleanDbs()
        self.restoreWithRename(backdir, {self.DB_V: newV})

        ddl = self.vtableDdl(newV)
        assert f"`{self.DB_P}`." in ddl, \
            f"vn DDL should still reference untouched {self.DB_P}: {ddl!r}"
        tdSql.query(f"select * from {newV}.vn")
        tdSql.checkRows(2)

        # stream lives in (unrenamed) DB_P, source table is in DB_P too, target
        # table is in (renamed) DB_V — only the target reference moves
        sddl = self.streamDdl()
        assert f"{newV}." in sddl and f"{self.DB_V}." not in sddl, \
            f"stream target was not rewritten to {newV}: {sddl!r}"
        assert f"{self.DB_P}." in sddl, \
            f"stream source should still reference untouched {self.DB_P}: {sddl!r}"

        # topic only references DB_P, so renaming DB_V must not affect it
        tdSql.query(f"select topic_name, db_name from information_schema.ins_topics "
                    f"where topic_name='{self.TOPIC}'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, self.DB_P)

        tdSql.execute(f"drop topic if exists {self.TOPIC}")
        tdSql.execute(f"drop database if exists {newV}")
        tdSql.execute(f"drop database if exists {self.DB_P}")
        tdLog.info("-W renames only the referencing db ..... [passed]")

    def do_rename_referenced_db_only(self, tmpdir):
        """-W renames only the database a virtual table READS FROM (DB_P), which
        is also the stream's own database and the topic's own database; the
        database the virtual table LIVES IN (DB_V) is left untouched.

        This is the core of the fix: DB_P is "someone else's database" from
        DB_V's point of view, so rewriting it requires applying the FULL -W
        mapping while restoring DB_V, not just the pair for DB_V itself.
        """
        backdir = os.path.join(tmpdir, "all")
        newP = "zz_renamed_p"

        self.cleanDbs()
        self.restoreWithRename(backdir, {self.DB_P: newP})

        # virtual table lives in (unrenamed) DB_V, reads from (renamed) newP
        ddl = self.vtableDdl(self.DB_V)
        assert f"`{self.DB_P}`." not in ddl and f"`{newP}`." in ddl, \
            f"vn DDL was not rewritten to {newP}: {ddl!r}"
        tdSql.query(f"select * from {self.DB_V}.vn")
        tdSql.checkRows(2)

        # stream lives in (renamed) newP, source table is in newP too, target
        # table is in (unrenamed) DB_V — only the source reference moves
        sddl = self.streamDdl()
        assert f"{newP}." in sddl and f"{self.DB_P}." not in sddl, \
            f"stream source was not rewritten to {newP}: {sddl!r}"
        assert f"{self.DB_V}." in sddl, \
            f"stream target should still reference untouched {self.DB_V}: {sddl!r}"

        # topic's query only ever references DB_P
        tdSql.query(f"select topic_name, db_name from information_schema.ins_topics "
                    f"where topic_name='{self.TOPIC}'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, newP)

        tdSql.execute(f"drop topic if exists {self.TOPIC}")
        tdSql.execute(f"drop database if exists {self.DB_V}")
        tdSql.execute(f"drop database if exists {newP}")
        tdLog.info("-W renames only the referenced db ...... [passed]")

    def do_rename_both_dbs(self, tmpdir):
        """-W renames both the referencing and the referenced database at once.

        -W carries the full old-db->new-db mapping, and the restore applies
        every configured pair to virtual table / stream / topic DDL, not just
        the pair for the database currently being restored — so DB_V's virtual
        table reference to DB_P, and the stream's cross-db target, both move
        to their new names together with the databases themselves.
        """
        backdir = os.path.join(tmpdir, "all")
        newP = "zz_renamed_p2"
        newV = "zz_renamed_v2"

        self.cleanDbs()
        self.restoreWithRename(backdir, {self.DB_P: newP, self.DB_V: newV})

        # virtual table's cross-db reference must follow DB_P's new name —
        # before applying every -W pair this stayed pointed at DB_P and the
        # restore failed outright once DB_P no longer existed under its old name
        ddl = self.vtableDdl(newV)
        assert f"`{self.DB_P}`." not in ddl and f"`{newP}`." in ddl, \
            f"vn DDL was not rewritten to {newP}: {ddl!r}"
        tdSql.query(f"select * from {newV}.vn")
        tdSql.checkRows(2)

        # stream: both source (own db) and target (other db) move
        sddl = self.streamDdl()
        assert f"{newP}." in sddl and f"{self.DB_P}." not in sddl, \
            f"stream source was not rewritten to {newP}: {sddl!r}"
        assert f"{newV}." in sddl and f"{self.DB_V}." not in sddl, \
            f"stream target was not rewritten to {newV}: {sddl!r}"

        tdSql.query(f"select topic_name, db_name from information_schema.ins_topics "
                    f"where topic_name='{self.TOPIC}'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, newP)

        tdSql.execute(f"drop topic if exists {self.TOPIC}")
        tdSql.execute(f"drop database if exists {newV}")
        tdSql.execute(f"drop database if exists {newP}")
        tdLog.info("-W renames both referencing and referenced db [passed]")

    #
    # ------------------- main ----------------
    #

    def test_taosbackup_content(self):
        """taosdump content selection and two-stage restore

        Verifies -M/--content (basic|ext-meta|all) and that the restore applies
        extended metadata (virtual tables / streams / topics) only after every
        database's physical tables exist, so cross-database virtual table
        references resolve regardless of database ordering.

        1. Cross-db virtual tables restored with the referencing database listed
           first — the regression guard for the ordering fix
        2. Default content is basic: no vtb.sql / stream.sql / topic.sql, but
           vtags/ is still backed up for a database with a virtual STB
        3. A basic pass followed by an ext-meta pass equals a single all pass
        4. --content=ext-meta re-creates a missing database before applying DDL
        5. An ext-meta-only backup writes db.sql (needed for discovery) and no data
        6. An unknown --content value is rejected
        7. -W renames only the database a virtual table lives in (DB_V); its
           cross-db reference to the untouched DB_P stays as-is
        8. -W renames only the database a virtual table/stream/topic reads
           from (DB_P); the reference is rewritten even though DB_V itself
           (where the virtual table lives) is not being renamed
        9. -W renames both databases at once; every cross-db reference
           (virtual table, and the stream's cross-db target) follows

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-08-07 Added with the two-stage restore / --content feature

        """
        tmpdir = tempfile.mkdtemp(prefix="taosdump_content_")

        self.prepareData()
        self.do_reverse_order_restore(tmpdir)
        self.do_default_is_basic(tmpdir)
        self.do_split_restore(tmpdir)
        self.do_extmeta_only_creates_db(tmpdir)
        self.do_extmeta_only_backup(tmpdir)
        self.do_invalid_content(tmpdir)
        self.do_rename_referencing_db_only(tmpdir)
        self.do_rename_referenced_db_only(tmpdir)
        self.do_rename_both_dbs(tmpdir)
