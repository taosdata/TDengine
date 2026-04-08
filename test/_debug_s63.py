#!/usr/bin/env python3
"""Debug s63: STB + child tables mixed chain + COMMIT"""
import taos, subprocess, time, os, signal

# Kill existing taosd
os.system("pkill -9 -f taosd 2>/dev/null")
time.sleep(2)

# Start taosd
proc = subprocess.Popen(["taosd"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
time.sleep(3)

conn = taos.connect()
c = conn.cursor()

c.execute("drop database if exists txn_db")
c.execute("create database txn_db vgroups 2")
c.execute("use txn_db")

print("Setup done.")

c.execute("BEGIN")
print("BEGIN done.")

c.execute("create table stb1 (ts timestamp, c0 int) tags(t0 int)")
print("CREATE STB done.")

c.execute("create table ct1 using stb1 tags(1)")
print("CREATE CT1 done.")

c.execute("create table ct2 using stb1 tags(2)")
print("CREATE CT2 done.")

# Same-txn visibility
c.execute("show tables")
rows = c.fetchall()
print(f"show tables within txn: {len(rows)} rows")

c.execute("COMMIT")
print("COMMIT done.")

# After commit
try:
    c.execute("show txn_db.stables")
    rows = c.fetchall()
    print(f"show stables after commit: {len(rows)} rows")
except Exception as e:
    print(f"show stables FAILED: {e}")

try:
    c.execute("show tables")
    rows = c.fetchall()
    print(f"show tables after commit: {len(rows)} rows")
except Exception as e:
    print(f"show tables FAILED: {e}")

# Try insert
try:
    c.execute("insert into ct1 values(now, 1)")
    print("INSERT ct1 done.")
except Exception as e:
    print(f"INSERT ct1 FAILED: {e}")

try:
    c.execute("select count(*) from stb1")
    rows = c.fetchall()
    print(f"select count: {rows}")
except Exception as e:
    print(f"select FAILED: {e}")

c.close()
conn.close()
os.kill(proc.pid, signal.SIGTERM)
