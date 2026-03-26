#!/usr/bin/env python3
# filepath: hot_update/resource/resourceManager.py
#
# Creates and validates all test resources needed for the rolling-upgrade test:
#   - Database (3-replica)
#   - Super table + 1 000 subtables
#   - Initial data (100 000 rows per subtable) via parallel batch inserts
#   - TMQ topic

import os
import sys
import time
import random
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

# Make sure the parent directory is on sys.path so `import config` works
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# ---------------------------------------------------------------------------
# Expected / ignorable errno values for idempotent DDL operations.
# Source: taoserror.h  TAOS_DEF_ERROR_CODE(0, code) -> errno & 0xffff == code
#
# Most DROP statements use IF EXISTS syntax.  However some older server
# versions (e.g. 3.3.6.0) have a bug where IF EXISTS does not suppress the
# "not exist" error and still returns one of the codes below.  We keep a
# small whitelist for those cases.
#
# CREATE SNODE has no IF NOT EXISTS grammar at all, so it always needs the
# errno-based filter.
# ---------------------------------------------------------------------------
_ERRNO_SNODE_ALREADY_EXISTS = frozenset({
    0x03A4,   # TSDB_CODE_MND_SNODE_ALREADY_EXIST
    0x040F,   # TSDB_CODE_SNODE_ALREADY_DEPLOYED
})

# Older-server "not exist" codes that IF EXISTS failed to suppress.
_ERRNO_IF_EXISTS_BUG = frozenset({
    0x0481,   # TSDB_CODE_MND_SMA_NOT_EXIST   (old SMA layer, seen on 3.3.6.0 DROP TSMA)
    0x3162,   # TSDB_CODE_RSMA_NOT_EXIST
    0x03F1,   # TSDB_CODE_MND_STREAM_NOT_EXIST
})

import taos
import config
from server.clusterSetup import Logger
from resource.priv_compat import grant_keywords, revoke_default_role, snapshot_privileges

_RETRY_COUNT = 10
_RETRY_INTERVAL = 1  # seconds


def _errno_of(exc) -> int:
    """Return the low-16-bit errno of a taos exception, or -1 for non-taos."""
    raw = getattr(exc, "errno", None)
    if raw is None:
        return -1
    return int(raw) & 0xFFFF


def _make_conn(fqdn: str, cfg_dir: str, logger=None) -> taos.TaosConnection:
    """Connect to TDengine with retry on transient errors."""
    last_exc = None
    for attempt in range(1, _RETRY_COUNT + 1):
        try:
            return taos.connect(host=fqdn, config=cfg_dir)
        except Exception as e:
            last_exc = e
            msg = f"  Connection failed (attempt {attempt}/{_RETRY_COUNT}): {e}, retrying in {_RETRY_INTERVAL}s ..."
            if logger:
                logger.info(msg)
            else:
                print(msg, file=sys.stderr)
            time.sleep(_RETRY_INTERVAL)
    raise RuntimeError(
        f"Failed to connect after {_RETRY_COUNT} attempts. Last error: {last_exc}"
    )


def _exec(cursor, sql: str, *,
          ignore: frozenset = frozenset(),
          logger=None) -> bool:
    """
    Execute *sql* on *cursor* with retry on transient server errors.

    Parameters
    ----------
    cursor  : taos cursor
    sql     : SQL text to execute
    ignore  : frozenset of errno values (low 16 bits) that are expected and
              should be silently skipped (logged at INFO level).  All other
              errors are retried up to _RETRY_COUNT times, then re-raised.
    logger  : optional Logger for the INFO notice; falls back to stderr.

    Returns
    -------
    True  – statement executed successfully
    False – error was in *ignore* and was suppressed
    """
    last_exc = None
    for attempt in range(1, _RETRY_COUNT + 1):
        try:
            cursor.execute(sql)
            return True
        except Exception as exc:
            code = _errno_of(exc)
            if code in ignore:
                msg = f"  SQL ignored [0x{code:04x}] ({exc}): {sql}"
                if logger:
                    logger.info(msg)
                else:
                    print(msg, file=sys.stderr)
                return False
            last_exc = exc
            msg = (
                f"  SQL failed [0x{code:04x}] (attempt {attempt}/{_RETRY_COUNT}): {exc} | SQL: {sql}"
                if attempt < _RETRY_COUNT
                else f"SQL failed after {_RETRY_COUNT} attempts [0x{code:04x}] ({exc}): {sql}"
            )
            if logger:
                logger.info(msg) if attempt < _RETRY_COUNT else logger.error(msg)
            else:
                print(f"{'WARN' if attempt < _RETRY_COUNT else 'ERROR'}: {msg}", file=sys.stderr)
            if attempt < _RETRY_COUNT:
                time.sleep(_RETRY_INTERVAL)

    raise last_exc


# ==================== ResourceManager ====================

class ResourceManager:
    """
    Idempotent creator of all SQL resources needed for the rolling-upgrade test.
    Call create_all() once after the cluster is up.
    """

    def __init__(self, fqdn: str, cfg_dir: str, logger: Optional[Logger] = None):
        self.fqdn    = fqdn
        self.cfg_dir = cfg_dir
        self.logger  = logger or Logger()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def create_all(self):
        """Run every preparation step in order."""
        self.create_database()
        self.create_stable()
        self.create_subtables()
        self.write_initial_data()
        self.create_topic()
        self.logger.success("All resources created successfully.")

    # ------------------------------------------------------------------
    # Step 1 – Database
    # ------------------------------------------------------------------

    def create_database(self):
        self.logger.info(f"Creating database '{config.DB_NAME}' with replica={config.REPLICA} ...")
        conn = _make_conn(self.fqdn, self.cfg_dir, self.logger)
        cursor = conn.cursor()
        try:
            _exec(cursor,
                  f"CREATE DATABASE IF NOT EXISTS {config.DB_NAME} "
                  f"REPLICA {config.REPLICA} "
                  f"VGROUPS {config.VGROUPS} "
                  f"STT_TRIGGER 2 "
                  f"WAL_RETENTION_PERIOD 3600 ",
                  logger=self.logger)
            _exec(cursor, f"USE {config.DB_NAME}", logger=self.logger)
            self.logger.success(f"Database '{config.DB_NAME}' ready.")
        finally:
            cursor.close()
            conn.close()

    # ------------------------------------------------------------------
    # Step 2 – Super table
    # ------------------------------------------------------------------

    def create_stable(self):
        self.logger.info(f"Creating stable '{config.STABLE_NAME}' ...")
        conn = _make_conn(self.fqdn, self.cfg_dir, self.logger)
        cursor = conn.cursor()
        try:
            _exec(cursor, f"USE {config.DB_NAME}", logger=self.logger)
            _exec(cursor,
                  f"CREATE STABLE IF NOT EXISTS {config.STABLE_NAME} ("
                  f"  ts        TIMESTAMP,"
                  f"  current   FLOAT,"
                  f"  voltage   INT,"
                  f"  phase     FLOAT"
                  f") TAGS ("
                  f"  groupid   INT,"
                  f"  location  BINARY(64)"
                  f")",
                  logger=self.logger)
            self.logger.success(f"Stable '{config.STABLE_NAME}' ready.")
        finally:
            cursor.close()
            conn.close()

    # ------------------------------------------------------------------
    # Step 3 – Sub tables
    # ------------------------------------------------------------------

    def create_subtables(self):
        self.logger.info(f"Creating {config.SUBTABLE_COUNT} subtables ...")
        conn = _make_conn(self.fqdn, self.cfg_dir, self.logger)
        cursor = conn.cursor()
        try:
            _exec(cursor, f"USE {config.DB_NAME}", logger=self.logger)
            # Build a single USING … TAGS multi-create statement per batch of 200
            batch_size = 200
            locations  = ["BeiJing", "ShangHai", "ShenZhen", "ChengDu", "HangZhou"]
            for batch_start in range(0, config.SUBTABLE_COUNT, batch_size):
                batch_end = min(batch_start + batch_size, config.SUBTABLE_COUNT)
                parts = []
                for i in range(batch_start, batch_end):
                    loc = locations[i % len(locations)]
                    parts.append(
                        f"d{i} USING {config.STABLE_NAME} TAGS ({i % 10}, '{loc}_{i}')"
                    )
                sql = f"CREATE TABLE IF NOT EXISTS {' '.join(parts)}"
                _exec(cursor, sql, logger=self.logger)
            self.logger.success(f"{config.SUBTABLE_COUNT} subtables ready.")
        finally:
            cursor.close()
            conn.close()

    # ------------------------------------------------------------------
    # Step 4 – Initial data (parallel batch inserts)
    # ------------------------------------------------------------------

    def write_initial_data(self):
        total = config.SUBTABLE_COUNT * config.INIT_ROWS_PER_SUBTABLE
        self.logger.info(
            f"Writing initial data: {config.SUBTABLE_COUNT} subtables × "
            f"{config.INIT_ROWS_PER_SUBTABLE} rows = {total:,} rows total "
            f"(threads={config.WRITE_THREADS}, batch={config.WRITE_BATCH_SIZE}) ..."
        )

        # Assign subtables to threads
        indices = list(range(config.SUBTABLE_COUNT))
        random.shuffle(indices)

        done_count  = [0]
        done_lock   = threading.Lock()
        errors      = []

        def write_subtable(idx: int):
            """Write INIT_ROWS_PER_SUBTABLE rows for subtable d{idx}."""
            try:
                conn   = _make_conn(self.fqdn, self.cfg_dir, self.logger)
                cursor = conn.cursor()
                _exec(cursor, f"USE {config.DB_NAME}", logger=self.logger)

                rows      = config.INIT_ROWS_PER_SUBTABLE
                batch     = config.WRITE_BATCH_SIZE
                # Timestamps: rows seconds ago → now (exclusive), spaced 1 s apart
                now_ms    = int(time.time() * 1000)
                start_ms  = now_ms - rows * 1000

                for offset in range(0, rows, batch):
                    end = min(offset + batch, rows)
                    vals = []
                    for j in range(offset, end):
                        ts      = start_ms + j * 1000
                        current = round(random.uniform(10.0, 15.0), 3)
                        voltage = random.randint(200, 240)
                        phase   = round(random.uniform(0.0, 1.0), 3)
                        vals.append(f"({ts}, {current}, {voltage}, {phase})")
                    sql = f"INSERT INTO d{idx} VALUES {', '.join(vals)}"
                    _exec(cursor, sql, logger=self.logger)

                cursor.close()
                conn.close()

                with done_lock:
                    done_count[0] += 1
                    if done_count[0] % 100 == 0:
                        self.logger.info(
                            f"  Progress: {done_count[0]}/{config.SUBTABLE_COUNT} subtables written"
                        )
            except Exception as e:
                errors.append(f"d{idx}: {e}")

        with ThreadPoolExecutor(max_workers=config.WRITE_THREADS) as executor:
            futures = {executor.submit(write_subtable, idx): idx for idx in indices}
            for future in as_completed(futures):
                exc = future.exception()
                if exc:
                    errors.append(str(exc))

        if errors:
            raise RuntimeError(
                f"Initial data write failed for {len(errors)} subtables. "
                f"First error: {errors[0]}"
            )

        self.logger.success(
            f"Initial data written: {config.SUBTABLE_COUNT} subtables × "
            f"{config.INIT_ROWS_PER_SUBTABLE} rows."
        )

    # ------------------------------------------------------------------
    # Step 5 – Topic
    # ------------------------------------------------------------------

    def create_topic(self):
        self.logger.info(f"Creating topic '{config.TOPIC_NAME}' ...")
        conn = _make_conn(self.fqdn, self.cfg_dir, self.logger)
        cursor = conn.cursor()
        try:
            _exec(cursor, f"USE {config.DB_NAME}", logger=self.logger)
            _exec(cursor,
                  f"CREATE TOPIC IF NOT EXISTS {config.TOPIC_NAME} "
                  f"AS SELECT * FROM {config.STABLE_NAME}",
                  logger=self.logger)
            self.logger.success(f"Topic '{config.TOPIC_NAME}' ready.")
        finally:
            cursor.close()
            conn.close()

    # ------------------------------------------------------------------
    # Step 6 – Test user + privilege grants
    # Creates:
    #   • Three stables with the same schema as STABLE_NAME:
    #       meters_ro   → test_user has SELECT only
    #       meters_wo   → test_user has INSERT only
    #       meters_hidden → test_user has NO access
    #   • One subtable under each stable
    #   • User test_user with SYSINFO 0
    #   • GRANT SELECT ON test_db.meters_ro TO test_user
    #   • GRANT INSERT ON test_db.meters_wo TO test_user
    # ------------------------------------------------------------------

    def create_test_user(self) -> frozenset:
        """
        Set up test_user and its restricted privileges.
        Returns a frozenset of (priv_type, priv_scope, db_name, table_name) tuples
        representing the exact privilege set granted – to be compared after upgrade.
        """
        self.logger.info(
            f"Creating test user '{config.TEST_USER_NAME}' with restricted privileges ..."
        )
        conn   = _make_conn(self.fqdn, self.cfg_dir, self.logger)
        cursor = conn.cursor()
        try:
            _exec(cursor, f"USE {config.DB_NAME}", logger=self.logger)

            # ---- create the three permission-test stables ----
            schema = (
                "(ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) "
                "TAGS (groupid INT, location BINARY(64))"
            )
            for sname in (
                config.TEST_USER_READ_STABLE,
                config.TEST_USER_WRITE_STABLE,
                config.TEST_USER_HIDDEN_STABLE,
            ):
                _exec(cursor,
                      f"CREATE STABLE IF NOT EXISTS {config.DB_NAME}.{sname} {schema}",
                      logger=self.logger)

            # ---- create one subtable under each stable ----
            _exec(cursor,
                  f"CREATE TABLE IF NOT EXISTS {config.DB_NAME}.d_ro0 "
                  f"USING {config.DB_NAME}.{config.TEST_USER_READ_STABLE} TAGS(0, 'ro')",
                  logger=self.logger)
            _exec(cursor,
                  f"CREATE TABLE IF NOT EXISTS {config.DB_NAME}.d_wo0 "
                  f"USING {config.DB_NAME}.{config.TEST_USER_WRITE_STABLE} TAGS(0, 'wo')",
                  logger=self.logger)
            _exec(cursor,
                  f"CREATE TABLE IF NOT EXISTS {config.DB_NAME}.d_hidden0 "
                  f"USING {config.DB_NAME}.{config.TEST_USER_HIDDEN_STABLE} TAGS(0, 'hidden')",
                  logger=self.logger)

            # ---- create user ----
            _exec(cursor,
                  f"CREATE USER {config.TEST_USER_NAME} "
                  f"PASS '{config.TEST_USER_PASS}' SYSINFO 0",
                  logger=self.logger)

            # ---- grant privileges (version-aware) ----
            read_kw, write_kw = grant_keywords(cursor)
            self.logger.info(
                f"Server → using GRANT {read_kw}/{write_kw} syntax"
            )
            revoke_default_role(cursor, config.TEST_USER_NAME, self.logger)

            _exec(cursor,
                  f"GRANT {read_kw} ON {config.DB_NAME}.{config.TEST_USER_READ_STABLE} "
                  f"TO {config.TEST_USER_NAME}",
                  logger=self.logger)
            _exec(cursor,
                  f"GRANT {write_kw} ON {config.DB_NAME}.{config.TEST_USER_WRITE_STABLE} "
                  f"TO {config.TEST_USER_NAME}",
                  logger=self.logger)

            # ---- capture normalised privilege snapshot ----
            snap = snapshot_privileges(cursor, config.TEST_USER_NAME)
            self.logger.success(
                f"test_user '{config.TEST_USER_NAME}' created; "
                f"{len(snap)} privilege row(s) granted "
                f"({read_kw} on {config.TEST_USER_READ_STABLE}, "
                f"{write_kw} on {config.TEST_USER_WRITE_STABLE})"
            )
            return snap

        finally:
            cursor.close()
            conn.close()

    # ------------------------------------------------------------------
    # Step 7 – Tag index
    # Creates one explicit tag index on the main stable:
    #   idx_compat_location → meters.location
    # (meters.groupid is the first tag and already has an auto-generated index;
    # attempting to add another index on it raises 0x0483 "index already exists".)
    # Returns a snapshot (dict) to be compared after upgrade.
    # ------------------------------------------------------------------

    def _show_index_snapshot(self, cursor) -> dict:
        """
        Run ``SHOW INDEXES FROM <stable> FROM <db>`` and return a dict keyed
        by index name.  Values are raw row tuples (positional) so no column
        name string is relied upon for cross-version comparisons.

        The column that holds the index name is located via cursor.description
        (case-insensitive search for "index_name"); position 2 is the fallback
        per the documented schema: db_name, table_name, index_name, …
        """
        _exec(cursor,
              f"SHOW INDEXES FROM {config.STABLE_NAME} FROM {config.DB_NAME}")
        desc = [d[0].lower() for d in cursor.description]
        try:
            name_pos = next(i for i, c in enumerate(desc) if c == "index_name")
        except StopIteration:
            name_pos = 2  # documented position fallback

        target = {config.TAG_INDEX_LOCATION}
        return {
            row[name_pos]: tuple(row)
            for row in cursor.fetchall()
            if row[name_pos] in target
        }

    def create_tag_indexes(self) -> dict:
        """
        Create two tag indexes on the main stable and return a positional
        snapshot from ``SHOW INDEXES`` to be compared after upgrade.

        Returns:
            dict[str, tuple]: index_name → row tuple (positional)
        """
        self.logger.info("Creating tag indexes on stable 'meters' ...")
        conn   = _make_conn(self.fqdn, self.cfg_dir, self.logger)
        cursor = conn.cursor()
        try:
            _exec(cursor, f"USE {config.DB_NAME}", logger=self.logger)
            _exec(cursor,
                  f"CREATE INDEX {config.TAG_INDEX_LOCATION} "
                  f"ON {config.STABLE_NAME} (location)",
                  logger=self.logger)

            snapshot = self._show_index_snapshot(cursor)
            self.logger.success(
                f"Tag indexes created: {sorted(snapshot.keys())}"
            )
            return snapshot

        finally:
            cursor.close()
            conn.close()

    def verify_tag_indexes(self, snapshot: dict) -> tuple:
        """
        Re-query indexes via ``SHOW INDEXES`` after upgrade and compare
        against *snapshot* (positional row tuples from Phase 2).

        Rows are compared positionally up to the length of the baseline tuple
        so that harmlessly added trailing columns do not cause false failures.

        Returns:
            (True,  summary_str)  – all indexes present and values unchanged
            (False, error_str)    – one or more indexes missing or differ
        """
        if not snapshot:
            return False, "baseline snapshot is empty – Phase 2 did not capture indexes"

        conn   = _make_conn(self.fqdn, self.cfg_dir, self.logger)
        cursor = conn.cursor()
        try:
            current = self._show_index_snapshot(cursor)

            errors = []
            for name, expected_row in sorted(snapshot.items()):
                if name not in current:
                    errors.append(f"index '{name}' missing after upgrade")
                    continue
                got_row = current[name]
                n = len(expected_row)  # compare only baseline columns
                if tuple(got_row[:n]) != tuple(expected_row[:n]):
                    errors.append(
                        f"index '{name}' values changed: "
                        f"expected {expected_row[:n]} got {got_row[:n]}"
                    )

            if errors:
                return False, "; ".join(errors)

            names = sorted(current.keys())
            return True, f"all {len(names)} tag index(es) intact: {names}"

        finally:
            cursor.close()
            conn.close()

    # ------------------------------------------------------------------
    # Step 9 – TSMA
    # Creates one TSMA on the main stable.
    # Snapshot uses SHOW {db}.TSMAS (immediately consistent; no column-name
    # dependency).  Key fields snapshotted: tsma_name, db_name, table_name,
    # func_list, interval
    # ------------------------------------------------------------------

    _TSMA_SNAP_FIELDS = {"tsma_name", "db_name", "table_name", "func_list", "interval"}

    def _show_tsma_row(self, cursor, tsma_name: str):
        """
        Run ``SHOW {db}.TSMAS``, return a dict of snapshot fields for the
        named TSMA, or None if not found.  Column positions resolved
        dynamically via cursor.description.
        """
        _exec(cursor, f"SHOW {config.DB_NAME}.TSMAS")
        desc = [d[0].lower() for d in cursor.description]
        try:
            name_pos = next(i for i, c in enumerate(desc) if c == "tsma_name")
        except StopIteration:
            name_pos = 0

        snap_positions = {
            col: i for i, col in enumerate(desc)
            if col in self._TSMA_SNAP_FIELDS
        }

        for row in cursor.fetchall():
            if str(row[name_pos]) == tsma_name:
                return {field: str(row[pos]) for field, pos in snap_positions.items()}
        return None

    def create_tsma(self) -> dict:
        """
        Create a TSMA on the main stable and snapshot its key metadata via
        ``SHOW {db}.TSMAS`` for post-upgrade comparison.

        Returns:
            dict[str, dict]: tsma_name → {field: value} snapshot dict
        """
        self.logger.info(
            f"Creating TSMA '{config.TSMA_NAME}' on stable '{config.STABLE_NAME}' ..."
        )
        conn   = _make_conn(self.fqdn, self.cfg_dir, self.logger)
        cursor = conn.cursor()
        try:
            _exec(cursor, f"USE {config.DB_NAME}", logger=self.logger)

            # TSMA uses stream computation internally which requires a Snode.
            # Create one on dnode 1 (idempotent: ignore "already exists").
            _exec(cursor, "CREATE SNODE ON DNODE 1",
                  ignore=_ERRNO_SNODE_ALREADY_EXISTS, logger=self.logger)

            # Drop TSMA first for idempotency.
            # Older servers (e.g. 3.3.6.0) have a bug where IF EXISTS does not
            # suppress 0x0481 (MND_SMA_NOT_EXIST), so we ignore it explicitly.
            _exec(cursor, f"DROP TSMA IF EXISTS {config.TSMA_NAME}",
                  ignore=_ERRNO_IF_EXISTS_BUG, logger=self.logger)

            _exec(cursor,
                  f"CREATE TSMA {config.TSMA_NAME} "
                  f"ON {config.DB_NAME}.{config.STABLE_NAME} "
                  f"FUNCTION(sum(voltage), avg(current), min(voltage), max(voltage), "
                  f"count(ts), last(phase)) "
                  f"INTERVAL(1m)",
                  logger=self.logger)

            snap = self._show_tsma_row(cursor, config.TSMA_NAME)
            if snap is None:
                raise RuntimeError(
                    f"SHOW TSMAS returned nothing for '{config.TSMA_NAME}'"
                )

            self.logger.success(
                f"TSMA '{config.TSMA_NAME}' created; "
                f"interval={snap.get('interval')}, func_list={snap.get('func_list')}"
            )
            return {config.TSMA_NAME: snap}

        finally:
            cursor.close()
            conn.close()

    def verify_tsma(self, snapshot: dict) -> tuple:
        """
        Re-query ``SHOW {db}.TSMAS`` after upgrade and compare key fields
        against *snapshot* captured in Phase 2.

        Returns:
            (True,  summary_str)  – TSMA present with all key fields unchanged
            (False, error_str)    – TSMA missing or a field changed
        """
        if not snapshot:
            return False, "baseline snapshot is empty – Phase 2 did not capture TSMA"

        conn   = _make_conn(self.fqdn, self.cfg_dir, self.logger)
        cursor = conn.cursor()
        try:
            errors = []
            for name, expected in sorted(snapshot.items()):
                got = self._show_tsma_row(cursor, name)
                if got is None:
                    errors.append(f"TSMA '{name}' missing after upgrade")
                    continue
                for field, exp_val in expected.items():
                    got_val = got.get(field)
                    if str(exp_val) != str(got_val):
                        errors.append(
                            f"TSMA '{name}' field '{field}' changed: "
                            f"{exp_val!r} → {got_val!r}"
                        )

            if errors:
                return False, "; ".join(errors)

            names = sorted(snapshot.keys())
            return True, f"all {len(names)} TSMA(s) intact: {names}"

        finally:
            cursor.close()
            conn.close()

    # ------------------------------------------------------------------
    # Step 8 – RSMA
    # Creates one RSMA on the main stable covering current/voltage/phase.
    # Snapshot uses SHOW {db}.RSMAS (immediately consistent; no DDL string
    # comparison so minor formatting changes across versions don't fail us).
    # Key fields snapshotted: rsma_name, db_name, table_name, interval, func_list
    # ------------------------------------------------------------------

    # Fields we care about (subset of SHOW RSMAS output; time/id columns excluded)
    _RSMA_SNAP_FIELDS = {"rsma_name", "db_name", "table_name", "interval", "func_list"}

    def _show_rsma_row(self, cursor, rsma_name: str):
        """
        Run ``SHOW {db}.RSMAS``, return a dict of snapshot fields for the
        named RSMA, or None if not found.  Column positions are resolved
        dynamically via cursor.description so renames don't break the check.
        """
        _exec(cursor, f"SHOW {config.DB_NAME}.RSMAS")
        desc = [d[0].lower() for d in cursor.description]
        try:
            name_pos = next(i for i, c in enumerate(desc) if c == "rsma_name")
        except StopIteration:
            name_pos = 0

        snap_positions = {
            col: i for i, col in enumerate(desc)
            if col in self._RSMA_SNAP_FIELDS
        }

        for row in cursor.fetchall():
            if str(row[name_pos]) == rsma_name:
                return {field: str(row[pos]) for field, pos in snap_positions.items()}
        return None

    def create_rsma(self) -> dict:
        """
        Create a RSMA on the main stable and snapshot its key metadata via
        ``SHOW {db}.RSMAS`` for post-upgrade comparison.

        Returns:
            dict[str, dict]: rsma_name → {field: value} snapshot dict
        """
        self.logger.info(
            f"Creating RSMA '{config.RSMA_NAME}' on stable '{config.STABLE_NAME}' ..."
        )
        conn   = _make_conn(self.fqdn, self.cfg_dir, self.logger)
        cursor = conn.cursor()
        try:
            _exec(cursor, f"USE {config.DB_NAME}", logger=self.logger)

            # Drop first for idempotency (IF EXISTS bug workaround same as TSMA)
            _exec(cursor, f"DROP RSMA IF EXISTS {config.RSMA_NAME}",
                  ignore=_ERRNO_IF_EXISTS_BUG, logger=self.logger)

            _exec(cursor,
                  f"CREATE RSMA {config.RSMA_NAME} "
                  f"ON {config.DB_NAME}.{config.STABLE_NAME} "
                  f"FUNCTION(min(voltage), avg(current), last(phase)) "
                  f"INTERVAL(1h, 5h)",
                  logger=self.logger)

            snap = self._show_rsma_row(cursor, config.RSMA_NAME)
            if snap is None:
                raise RuntimeError(
                    f"SHOW RSMAS returned nothing for '{config.RSMA_NAME}'"
                )

            self.logger.success(
                f"RSMA '{config.RSMA_NAME}' created; "
                f"interval={snap.get('interval')}, func_list={snap.get('func_list')}"
            )
            return {config.RSMA_NAME: snap}

        finally:
            cursor.close()
            conn.close()

    def verify_rsma(self, snapshot: dict) -> tuple:
        """
        Re-query ``SHOW {db}.RSMAS`` after upgrade and compare key fields
        against *snapshot* captured in Phase 2.

        Returns:
            (True,  summary_str)  – RSMA present with all key fields unchanged
            (False, error_str)    – RSMA missing or a field changed
        """
        if not snapshot:
            return False, "baseline snapshot is empty – Phase 2 did not capture RSMA"

        conn   = _make_conn(self.fqdn, self.cfg_dir, self.logger)
        cursor = conn.cursor()
        try:
            errors = []
            for name, expected in sorted(snapshot.items()):
                got = self._show_rsma_row(cursor, name)
                if got is None:
                    errors.append(f"RSMA '{name}' missing after upgrade")
                    continue
                for field, exp_val in expected.items():
                    got_val = got.get(field)
                    if str(exp_val) != str(got_val):
                        errors.append(
                            f"RSMA '{name}' field '{field}' changed: "
                            f"{exp_val!r} → {got_val!r}"
                        )

            if errors:
                return False, "; ".join(errors)

            names = sorted(snapshot.keys())
            return True, f"all {len(names)} RSMA(s) intact: {names}"

        finally:
            cursor.close()
            conn.close()

    # ------------------------------------------------------------------
    # Step 10 – Stream (全新流)
    # Creates one new-style PERIOD stream on the main stable.
    # The stream requires a Snode (already ensured by create_tsma).
    # Snapshot: stream_name + status from ``SHOW {db}.STREAMS``.
    # After upgrade we verify the stream still exists (status is not
    # checked strictly since it may transition briefly during upgrade).
    # ------------------------------------------------------------------

    _STREAM_SNAP_FIELDS = {"stream_name", "status"}

    def _show_stream_row(self, cursor, stream_name: str):
        """
        Run ``SHOW {db}.STREAMS``, return a dict of snapshot fields for the
        named stream, or None if not found.
        """
        _exec(cursor, f"SHOW {config.DB_NAME}.STREAMS")
        desc = [d[0].lower() for d in cursor.description]
        try:
            name_pos = next(i for i, c in enumerate(desc) if c == "stream_name")
        except StopIteration:
            name_pos = 0

        snap_positions = {
            col: i for i, col in enumerate(desc)
            if col in self._STREAM_SNAP_FIELDS
        }

        for row in cursor.fetchall():
            if str(row[name_pos]) == stream_name:
                return {field: str(row[pos]) for field, pos in snap_positions.items()}
        return None

    def create_stream(self) -> dict:
        """
        Create a new-style PERIOD stream on the main stable and snapshot
        its key metadata via ``SHOW {db}.STREAMS`` for post-upgrade comparison.

        Returns:
            dict[str, dict]: stream_name → {field: value} snapshot dict
        """
        self.logger.info(
            f"Creating Stream '{config.STREAM_NAME}' on stable '{config.STABLE_NAME}' ..."
        )
        conn   = _make_conn(self.fqdn, self.cfg_dir, self.logger)
        cursor = conn.cursor()
        try:
            # Snode already exists from create_tsma; streams need it too.
            _exec(cursor, "CREATE SNODE ON DNODE 1",
                  ignore=_ERRNO_SNODE_ALREADY_EXISTS, logger=self.logger)

            # Drop for idempotency (IF EXISTS bug workaround same as TSMA)
            _exec(cursor, f"DROP STREAM IF EXISTS {config.DB_NAME}.{config.STREAM_NAME}",
                  ignore=_ERRNO_IF_EXISTS_BUG, logger=self.logger)

            _exec(cursor, f"USE {config.DB_NAME}", logger=self.logger)

            # Drop the output table if it exists from a previous run
            _exec(cursor, f"DROP TABLE IF EXISTS {config.DB_NAME}.{config.STREAM_OUT_TABLE}",
                  logger=self.logger)

            # New-style PERIOD stream: every 1 minute compute aggregates over
            # the meters stable and write into an output table.
            _exec(cursor,
                  f"CREATE STREAM {config.STREAM_NAME} "
                  f"PERIOD(1m) "
                  f"FROM {config.DB_NAME}.{config.STABLE_NAME} "
                  f"INTO {config.DB_NAME}.{config.STREAM_OUT_TABLE} "
                  f"AS SELECT "
                  f"  CAST(_TLOCALTIME / 1000000 AS TIMESTAMP) AS ts, "
                  f"  AVG(current) AS avg_current, "
                  f"  MAX(voltage) AS max_voltage, "
                  f"  LAST(phase) AS last_phase "
                  f"FROM {config.DB_NAME}.{config.STABLE_NAME} "
                  f"WHERE ts >= _TPREV_LOCALTIME / 1000000 "
                  f"  AND ts <= _TNEXT_LOCALTIME / 1000000",
                  logger=self.logger)

            snap = self._show_stream_row(cursor, config.STREAM_NAME)
            if snap is None:
                raise RuntimeError(
                    f"SHOW STREAMS returned nothing for '{config.STREAM_NAME}'"
                )

            self.logger.success(
                f"Stream '{config.STREAM_NAME}' created; "
                f"status={snap.get('status')}"
            )
            return {config.STREAM_NAME: snap}

        finally:
            cursor.close()
            conn.close()

    def verify_stream(self, snapshot: dict) -> tuple:
        """
        Re-query ``SHOW {db}.STREAMS`` after upgrade and verify the stream
        still exists.  Status is allowed to be any non-empty value (it may
        transition briefly during the rolling upgrade).

        Returns:
            (True,  summary_str)  – stream present after upgrade
            (False, error_str)    – stream missing
        """
        if not snapshot:
            return False, "baseline snapshot is empty – Phase 2 did not capture stream"

        conn   = _make_conn(self.fqdn, self.cfg_dir, self.logger)
        cursor = conn.cursor()
        try:
            errors = []
            for name in sorted(snapshot.keys()):
                got = self._show_stream_row(cursor, name)
                if got is None:
                    errors.append(f"Stream '{name}' missing after upgrade")

            if errors:
                return False, "; ".join(errors)

            names = sorted(snapshot.keys())
            return True, f"all {len(names)} stream(s) intact: {names}"

        finally:
            cursor.close()
            conn.close()

    # ------------------------------------------------------------------
    # Utility – verify resource counts
    # ------------------------------------------------------------------

    def verify_resources(self) -> bool:
        """Quick sanity check: stable + subtables + topic exist."""
        conn   = _make_conn(self.fqdn, self.cfg_dir, self.logger)
        cursor = conn.cursor()
        ok     = True
        try:
            _exec(cursor, f"USE {config.DB_NAME}", logger=self.logger)

            _exec(cursor,
                  f"SELECT COUNT(*) FROM information_schema.ins_tables "
                  f"WHERE db_name='{config.DB_NAME}' AND stable_name='{config.STABLE_NAME}'",
                  logger=self.logger)
            row   = cursor.fetchone()
            count = row[0] if row else 0
            if count < config.SUBTABLE_COUNT:
                self.logger.error(
                    f"Expected {config.SUBTABLE_COUNT} subtables, found {count}"
                )
                ok = False
            else:
                self.logger.info(f"  Subtables: {count} ✓")

            _exec(cursor,
                  f"SELECT topic_name FROM information_schema.ins_topics "
                  f"WHERE topic_name='{config.TOPIC_NAME}'",
                  logger=self.logger)
            if not cursor.fetchone():
                self.logger.error(f"Topic '{config.TOPIC_NAME}' not found")
                ok = False
            else:
                self.logger.info(f"  Topic '{config.TOPIC_NAME}': ✓")

        finally:
            cursor.close()
            conn.close()
        return ok
