#!/usr/bin/env python3
"""Quick repro for s58 failure."""
import taos
import time
import subprocess, os, signal

# Start taosd
subprocess.Popen(["taosd"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
time.sleep(3)

conn = taos.connect()
try:
    conn.execute("drop database if exists txn_db")
    conn.execute("create database txn_db vgroups 2")
    conn.execute("use txn_db")
    
    # Setup: create STB outside txn
    conn.execute("create table stb1 (ts timestamp, c0 int) tags(t0 int)")
    conn.execute("create table ct1 using stb1 tags(1)")
    conn.execute("insert into ct1 values(now, 100)")
    print("Setup done.")
    
    # BEGIN
    conn.execute("BEGIN")
    print(f"BEGIN done. txnId should be > 0")
    
    # ALTER STB
    try:
        conn.execute("alter table stb1 add column c2 float")
        print("ALTER STB done.")
    except Exception as e:
        print(f"ALTER STB FAILED: {e}")
    
    # DROP STB
    try:
        conn.execute("drop table stb1")
        print("DROP STB done.")
    except Exception as e:
        print(f"DROP STB FAILED: {e}")
    
    # COMMIT
    try:
        conn.execute("COMMIT")
        print("COMMIT done.")
    except Exception as e:
        print(f"COMMIT FAILED: {e}")
    
    # Verify
    rows = conn.query("show txn_db.stables").fetch_all()
    print(f"STables after COMMIT: {len(rows)}")

except Exception as e:
    print(f"Error: {e}")
finally:
    conn.close()
    os.system("pkill -9 taosd 2>/dev/null")
