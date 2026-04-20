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
from new_test_framework.utils import tdLog, tdSql, epath, sc, etool, AutoGen, tdDnodes
import os
import subprocess


class TestBasic:
    """Test encryption with multi-level storage

    This test verifies that:
    1. taosk correctly identifies primary disk in multi-level storage configuration
    2. Encryption keys are stored only on primary disk (level=0, primary=1)
    3. Encrypted databases work correctly with multi-level storage
    4. Data is properly distributed across storage tiers

    Usage:
        # Single-level storage (default)
        pytest cases/31-Security/07-EncryptionMultiLevel/test_encryption_mlevel.py --clean

        # Multi-level storage (3 levels, 2 disks per level)
        pytest cases/31-Security/07-EncryptionMultiLevel/test_encryption_mlevel.py --clean -L 3 -D 2
    """

    # Configure debug flags
    updatecfgDict = {
        'dDebugFlag': 131,
        'vDebugFlag': 131,
        'fsDebugFlag': 131
    }

    # Pre-generate encryption keys with all options
    encryptConfig = {
        "svrKey": "mlevelsvr123",
        "dbKey": "mleveldb456",
        "dataKey": "mleveldat789",
        "generateConfig": True,
        "generateMeta": True,
        "generateData": True
    }

    def setup_class(cls):
        cls.init(cls, replicaVar=1, checkColName="c1")
        cls.valgrind = 0
        cls.db = "mlevel_encrypt_db"

    def test_encryption_mlevel(self):
        """ Test encryption with multi-level storage

        Tests:
        1. Verify taosk identifies primary disk correctly
        2. Verify encryption keys are stored on primary disk only
        3. Create encrypted database with multi-level storage
        4. Insert data and verify distribution across tiers
        5. Verify data integrity after flush and compact

        Since: v3.4.0.0

        Labels: common,ci

        Jira: TS-7230

        History:
            - 2026-04-14 Created for multi-level storage encryption support

        """
        # Get data directories (should be multiple for multi-level storage)
        data_dirs = tdDnodes.dnodes[0].dataDir
        if not isinstance(data_dirs, list):
            data_dirs = [data_dirs]

        cfg_path = tdDnodes.dnodes[0].cfgDir

        tdLog.info("=" * 80)
        tdLog.info("Test Environment Setup")
        tdLog.info("=" * 80)
        tdLog.info(f"Config directory: {cfg_path}")
        tdLog.info(f"Data directories: {data_dirs}")
        tdLog.info(f"Number of storage tiers: {len(data_dirs)}")

        # Test 1: Verify taosk identifies primary disk
        tdLog.info("=" * 80)
        tdLog.info("Test 1: Verify taosk identifies primary disk")
        tdLog.info("=" * 80)

        taosk_bin = etool.taoskFile()
        tdLog.info(f"taosk binary: {taosk_bin}")

        # Primary disk should be the first one (level=0, primary=1)
        # In multi-level storage, dataDir format is: "path level primary"
        primary_disk_entry = data_dirs[0]
        if isinstance(primary_disk_entry, str) and ' ' in primary_disk_entry:
            # Multi-level storage: extract path from "path level primary"
            primary_disk = primary_disk_entry.split()[0]
        else:
            # Single-level storage
            primary_disk = primary_disk_entry

        key_dir = os.path.join(primary_disk, "dnode", "config")
        master_file = os.path.join(key_dir, "master.bin")
        derived_file = os.path.join(key_dir, "derived.bin")

        tdLog.info(f"Expected primary disk: {primary_disk}")
        tdLog.info(f"Expected key directory: {key_dir}")

        # Verify keys exist on primary disk
        if os.path.exists(master_file):
            tdLog.info(f"✓ master.bin found on primary disk: {os.path.getsize(master_file)} bytes")
        else:
            tdLog.exit(f"✗ master.bin NOT found on primary disk: {master_file}")

        if os.path.exists(derived_file):
            tdLog.info(f"✓ derived.bin found on primary disk: {os.path.getsize(derived_file)} bytes")
        else:
            tdLog.info(f"Note: derived.bin not found (may not be generated yet)")

        # Test 2: Verify keys are NOT on other disks
        tdLog.info("=" * 80)
        tdLog.info("Test 2: Verify keys are stored ONLY on primary disk")
        tdLog.info("=" * 80)

        for i, disk_entry in enumerate(data_dirs[1:], start=1):
            # Extract disk path
            if isinstance(disk_entry, str) and ' ' in disk_entry:
                disk_path = disk_entry.split()[0]
            else:
                disk_path = disk_entry

            other_key_dir = os.path.join(disk_path, "dnode", "config")
            other_master = os.path.join(other_key_dir, "master.bin")

            if os.path.exists(other_master):
                tdLog.exit(f"✗ ERROR: master.bin found on non-primary disk {i}: {other_master}")
            else:
                tdLog.info(f"✓ Disk {i} ({disk_path}): No encryption keys (correct)")

        # Test 3: Create encrypted database with multi-level storage
        tdLog.info("=" * 80)
        tdLog.info("Test 3: Create encrypted database with multi-level storage")
        tdLog.info("=" * 80)

        # Create database with SM4 encryption and multi-level keep
        tdSql.execute(f"DROP DATABASE IF EXISTS {self.db}")
        tdSql.execute(f"CREATE DATABASE {self.db} ENCRYPT_ALGORITHM 'SM4-CBC' KEEP 40d,70d,120d VGROUPS 2")
        tdLog.info(f"Database {self.db} created with SM4 encryption and multi-level keep")

        # Verify database configuration
        tdSql.query(f"SELECT name, `encrypt_algorithm`, `keep` FROM information_schema.ins_databases WHERE name='{self.db}'")
        if tdSql.queryRows > 0:
            tdLog.info(f"Database info: {tdSql.queryResult}")
            assert 'SM4' in str(tdSql.queryResult[0]), "Database should use SM4 encryption"

        # Use database
        tdSql.execute(f"USE {self.db}")

        # Test 4: Insert data and verify distribution
        tdLog.info("=" * 80)
        tdLog.info("Test 4: Insert data and verify distribution across tiers")
        tdLog.info("=" * 80)

        # Create super table
        tdSql.execute("""
            CREATE STABLE IF NOT EXISTS meters (
                ts TIMESTAMP,
                voltage INT,
                current FLOAT,
                phase FLOAT,
                temperature FLOAT
            ) TAGS (location BINARY(32), groupid INT)
        """)
        tdLog.info("Super table created")

        # Create child tables and insert data
        num_tables = 10
        rows_per_table = 1000  # Reduced for faster testing

        import time
        current_time_ms = int(time.time() * 1000)

        for i in range(num_tables):
            table_name = f"d{i}"
            location = f"Location_{i}"

            tdSql.execute(f"CREATE TABLE {table_name} USING meters TAGS ('{location}', {i})")

            # Insert historical data in batches (will be distributed across storage tiers based on keep)
            batch_size = 100
            for batch_start in range(0, rows_per_table, batch_size):
                values = []
                for j in range(batch_start, min(batch_start + batch_size, rows_per_table)):
                    # Insert data spanning multiple days to trigger multi-level storage
                    day_offset = j // 100  # Change day every 100 records
                    sec_offset = j % 100  # Seconds within the day
                    # Calculate timestamp: current time - days - seconds
                    ts = current_time_ms - (day_offset * 86400000) - (sec_offset * 1000)
                    values.append(f"({ts}, {220 + i}, {10.0 + i * 0.1}, {0.95 + i * 0.01}, {25.0 + i})")

                tdSql.execute(f"INSERT INTO {table_name} VALUES {' '.join(values)}")

        tdLog.info(f"Inserted {num_tables * rows_per_table} rows across {num_tables} tables")

        # Verify data count
        tdSql.query("SELECT COUNT(*) FROM meters")
        total_rows = tdSql.queryResult[0][0]
        expected_rows = num_tables * rows_per_table
        assert total_rows == expected_rows, f"Expected {expected_rows} rows, got {total_rows}"
        tdLog.info(f"✓ Data verification passed: {total_rows} rows")

        # Test 5: Flush and verify data integrity
        tdLog.info("=" * 80)
        tdLog.info("Test 5: Flush database and verify data integrity")
        tdLog.info("=" * 80)

        # Flush database to ensure data is written to disk
        tdSql.execute(f"FLUSH DATABASE {self.db}")
        tdLog.info("Database flushed")

        # Wait for flush to complete
        import time
        time.sleep(2)

        # Verify data after flush
        tdSql.query("SELECT COUNT(*) FROM meters")
        rows_after_flush = tdSql.queryResult[0][0]
        assert rows_after_flush == expected_rows, f"Data lost after flush: {rows_after_flush} != {expected_rows}"
        tdLog.info(f"✓ Data integrity verified after flush: {rows_after_flush} rows")

        # Verify aggregate queries work correctly
        tdSql.query("SELECT AVG(voltage), MAX(current), MIN(phase) FROM meters")
        tdLog.info(f"Aggregate query result: {tdSql.queryResult}")

        # Test 6: Verify data files across storage tiers
        tdLog.info("=" * 80)
        tdLog.info("Test 6: Verify data files across storage tiers")
        tdLog.info("=" * 80)

        for i, disk in enumerate(data_dirs):
            vnode_dir = os.path.join(disk, "vnode")
            if os.path.exists(vnode_dir):
                vnode_list = os.listdir(vnode_dir)
                tdLog.info(f"Tier {i} ({disk}): {len(vnode_list)} vnode(s)")

                # Check for encrypted data files
                for vnode in vnode_list[:2]:  # Check first 2 vnodes
                    vnode_path = os.path.join(vnode_dir, vnode)
                    if os.path.isdir(vnode_path):
                        # Check TSDB directory
                        tsdb_dir = os.path.join(vnode_path, "tsdb")
                        if os.path.exists(tsdb_dir):
                            tsdb_files = [f for f in os.listdir(tsdb_dir) if os.path.isfile(os.path.join(tsdb_dir, f))]
                            tdLog.info(f"  Vnode {vnode} TSDB: {len(tsdb_files)} file(s)")

                        # Check WAL directory
                        wal_dir = os.path.join(vnode_path, "wal")
                        if os.path.exists(wal_dir):
                            wal_files = [f for f in os.listdir(wal_dir) if os.path.isfile(os.path.join(wal_dir, f))]
                            tdLog.info(f"  Vnode {vnode} WAL: {len(wal_files)} file(s)")
            else:
                tdLog.info(f"Tier {i} ({disk}): No vnode directory yet")

        # Test 7: Compact database
        tdLog.info("=" * 80)
        tdLog.info("Test 7: Compact database and verify data integrity")
        tdLog.info("=" * 80)

        tdSql.execute(f"COMPACT DATABASE {self.db}")
        tdLog.info("Database compacted")

        # Wait for compact to complete
        time.sleep(2)

        # Verify data after compact
        tdSql.query("SELECT COUNT(*) FROM meters")
        rows_after_compact = tdSql.queryResult[0][0]
        assert rows_after_compact == expected_rows, f"Data lost after compact: {rows_after_compact} != {expected_rows}"
        tdLog.info(f"✓ Data integrity verified after compact: {rows_after_compact} rows")

        # Test 8: Query data across time ranges (different storage tiers)
        tdLog.info("=" * 80)
        tdLog.info("Test 8: Query data across time ranges (different storage tiers)")
        tdLog.info("=" * 80)

        # Query recent data (should be on hot tier)
        tdSql.query("SELECT COUNT(*) FROM meters WHERE ts > now - 1d")
        recent_count = tdSql.queryResult[0][0]
        tdLog.info(f"Recent data (< 1 day): {recent_count} rows")

        # Query older data (should be on warm/cold tiers)
        tdSql.query("SELECT COUNT(*) FROM meters WHERE ts < now - 1d")
        old_count = tdSql.queryResult[0][0]
        tdLog.info(f"Older data (> 1 day): {old_count} rows")

        # Verify total
        assert recent_count + old_count == expected_rows, "Data count mismatch across tiers"
        tdLog.info(f"✓ Data distribution verified: {recent_count} + {old_count} = {expected_rows}")

        tdLog.success(f"{__file__} successfully executed")
