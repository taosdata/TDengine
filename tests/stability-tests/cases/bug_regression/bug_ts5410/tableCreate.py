import taos
# Target database settings for TDengine
target_ip = "127.0.0.1"
target_port = 6030
target_user = "root"
target_password = "taosdata"

table_count = 400
dbname = "DEFAULT_MEIOT_1"
super_table_name = "t_lingang_stable_sensor_shadow_pressure_1"

def get_subtables(conn, super_table):
    cursor = conn.cursor()
    try:
        for index in range(0, table_count, 1):
            tb_name = f"st_{index}"
            type = index % 30
            sensor_type = f"sensor_type_{type}"
            stage = index % 13
            next_stage = f"next_stage_{stage}"
            notes = f"notes_{index}"
            atmos_pressure_bar = index * 0.1
            param_a = index
            param_b = index * index
            
            sql = f"CREATE TABLE {dbname}.{tb_name} using {super_table}  tags (\"{sensor_type}\", \"{next_stage}\", \"{notes}\", {atmos_pressure_bar}, {param_a}, {param_b});"
            cursor.execute(sql)
            print(f"create table {dbname}.{tb_name}")
    except Exception as e:
        print(f"Failed to create table: {super_table}: {e}")
    finally:
        cursor.close()

def main():
    super_table = f"{dbname}.{super_table_name}"

    # Database connection
    print(f"Connecting to TDengine at {target_ip}:{target_port}...")
    conn = taos.connect(host=target_ip, port=target_port, user=target_user, password=target_password)
    print(f"Connected to TDengine at {target_ip}:{target_port}")
    cursor = conn.cursor()
    subtables = get_subtables(conn, super_table)

    # Close connection
    conn.close()
    print(f"finished, quit")
    


if __name__ == "__main__":
    main()