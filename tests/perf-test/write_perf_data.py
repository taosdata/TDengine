import os
import mysqldb
import insert_json
import query_json


if __name__ == "__main__":
    num_of_tables = 10000
    records_per_table = 10000
    interlace_rows = 0
    stt_trigger = 1
    
    db = mysqldb.MySQLDatabase()
    db.connect()
    sql = f"select id from scenarios where num_of_tables = {num_of_tables} and records_per_table = {records_per_table} and interlace_rows = {interlace_rows} and stt_trigger = {stt_trigger}"
    row = db.query(sql)
    if row is None:
        id = db.get_id(f"insert into scenarios(num_of_tables, records_per_table, interlace_rows, stt_trigger) values({num_of_tables},{records_per_table}, {interlace_rows}, {stt_trigger})")
    else:
        id = row[0][0]
        
    print(id)
    
    db.disconnect()
    
    insert = insert_json.InsertJson(num_of_tables, records_per_table, interlace_rows, stt_trigger)
    os.system(f"taosBenchmark -f {insert.create_insert_file()}")
    
    