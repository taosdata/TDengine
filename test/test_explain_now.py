"""Tiny test that just runs EXPLAIN VERBOSE TRUE and prints."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "new_test_framework"))

from utils.sql import tdSql
import taos

class TestExplainNow:
    def test_explain(self):
        conn = taos.connect(config="/tmp/sim_fq_warm/dnode1/cfg")
        cur = conn.cursor()

        for db in ["fq_parity_local", "fq_parity_src_m"]:
            sql = "EXPLAIN VERBOSE TRUE SELECT NOW() FROM %s.parity_t LIMIT 1" % db
            print("\n" + "=" * 70)
            print(sql)
            print("=" * 70)
            try:
                cur.execute(sql)
                rows = cur.fetchall()
                for row in rows:
                    print(row[0] if len(row) == 1 else " | ".join(str(c) for c in row))
            except Exception as e:
                print("ERROR: %s" % e)
        cur.close()
        conn.close()
