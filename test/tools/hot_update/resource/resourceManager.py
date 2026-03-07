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

import taos
import config
from server.clusterSetup import Logger


# ==================== helpers ====================

def _make_conn(fqdn: str, cfg_dir: str) -> taos.TaosConnection:
    return taos.connect(host=fqdn, config=cfg_dir)


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
        conn = _make_conn(self.fqdn, self.cfg_dir)
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"CREATE DATABASE IF NOT EXISTS {config.DB_NAME} "
                f"REPLICA {config.REPLICA} "
                f"WAL_RETENTION_PERIOD 3600 "
                f"WAL_LEVEL 2"
            )
            cursor.execute(f"USE {config.DB_NAME}")
            self.logger.success(f"Database '{config.DB_NAME}' ready.")
        finally:
            cursor.close()
            conn.close()

    # ------------------------------------------------------------------
    # Step 2 – Super table
    # ------------------------------------------------------------------

    def create_stable(self):
        self.logger.info(f"Creating stable '{config.STABLE_NAME}' ...")
        conn = _make_conn(self.fqdn, self.cfg_dir)
        cursor = conn.cursor()
        try:
            cursor.execute(f"USE {config.DB_NAME}")
            cursor.execute(
                f"CREATE STABLE IF NOT EXISTS {config.STABLE_NAME} ("
                f"  ts        TIMESTAMP,"
                f"  current   FLOAT,"
                f"  voltage   INT,"
                f"  phase     FLOAT"
                f") TAGS ("
                f"  groupid   INT,"
                f"  location  BINARY(64)"
                f")"
            )
            self.logger.success(f"Stable '{config.STABLE_NAME}' ready.")
        finally:
            cursor.close()
            conn.close()

    # ------------------------------------------------------------------
    # Step 3 – Sub tables
    # ------------------------------------------------------------------

    def create_subtables(self):
        self.logger.info(f"Creating {config.SUBTABLE_COUNT} subtables ...")
        conn = _make_conn(self.fqdn, self.cfg_dir)
        cursor = conn.cursor()
        try:
            cursor.execute(f"USE {config.DB_NAME}")
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
                cursor.execute(sql)
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
                conn   = _make_conn(self.fqdn, self.cfg_dir)
                cursor = conn.cursor()
                cursor.execute(f"USE {config.DB_NAME}")

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
                    cursor.execute(sql)

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
        conn = _make_conn(self.fqdn, self.cfg_dir)
        cursor = conn.cursor()
        try:
            cursor.execute(f"USE {config.DB_NAME}")
            cursor.execute(
                f"CREATE TOPIC IF NOT EXISTS {config.TOPIC_NAME} "
                f"AS SELECT * FROM {config.STABLE_NAME}"
            )
            self.logger.success(f"Topic '{config.TOPIC_NAME}' ready.")
        finally:
            cursor.close()
            conn.close()

    # ------------------------------------------------------------------
    # Utility – verify resource counts
    # ------------------------------------------------------------------

    def verify_resources(self) -> bool:
        """Quick sanity check: stable + subtables + topic exist."""
        conn   = _make_conn(self.fqdn, self.cfg_dir)
        cursor = conn.cursor()
        ok     = True
        try:
            cursor.execute(f"USE {config.DB_NAME}")

            cursor.execute(
                f"SELECT COUNT(*) FROM information_schema.ins_tables "
                f"WHERE db_name='{config.DB_NAME}' AND stable_name='{config.STABLE_NAME}'"
            )
            row   = cursor.fetchone()
            count = row[0] if row else 0
            if count < config.SUBTABLE_COUNT:
                self.logger.error(
                    f"Expected {config.SUBTABLE_COUNT} subtables, found {count}"
                )
                ok = False
            else:
                self.logger.info(f"  Subtables: {count} ✓")

            cursor.execute(
                f"SELECT topic_name FROM information_schema.ins_topics "
                f"WHERE topic_name='{config.TOPIC_NAME}'"
            )
            if not cursor.fetchone():
                self.logger.error(f"Topic '{config.TOPIC_NAME}' not found")
                ok = False
            else:
                self.logger.info(f"  Topic '{config.TOPIC_NAME}': ✓")

        finally:
            cursor.close()
            conn.close()
        return ok
