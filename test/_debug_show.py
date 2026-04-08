#!/usr/bin/env python3
"""Debug: show tables within txn where STB is also created in same txn"""
import taos, subprocess, time, os, signal

os.system("pkill -9 -f taosd 2>/dev/null")
time.sleep(2)
proc = subprocess.Popen(["taosd"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
time.sleep(3)

conn = taos.connect()
c = conn.cursor()

c.execute("drop database if exists txn_db")
time.sleep(1)
c.execute("create database txn_db vgroups 2")
c.execute("use txn_db")

print("=== Single test: STB+CTB both inside txn, show tables ===")
time.sleep(2)
c.execute("BEGIN")
c.execute("create table stb_in (ts timestamp, c0 int) tags(t0 int)")
c.execute("create table ct1 using stb_in tags(1)")
try:
    c.execute("show tables")
    rows = c.fetchall()
    print(f"  show tables: {len(rows)} rows (expect 1)")
except Exception as e:
    print(f"  show tables FAILED: {e}")
c.execute("ROLLBACK")

c.close()
conn.close()
os.kill(proc.pid, signal.SIGTERM)
print("Done.")
