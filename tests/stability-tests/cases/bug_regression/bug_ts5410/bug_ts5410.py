###################################################################
#           Copyright (c) 2020 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

from taostest import TDCase, T
from taostest.util.common import TDCom
from taostest.util.remote import Remote
import os
import random
import time

class TestTS5410(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.taosd_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "taosd")
        self.taospy_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "taospy")
        self.taosd_fqdn = self.taosd_setting["fqdn"]
        self.taospy_fqdn = self.taospy_setting["fqdn"]
        self._remote: Remote = Remote(self.logger)
        self._remote.cmd(self.taosd_fqdn[0], ["python3 -m pip install --upgrade pip"])
        self._remote.cmd(self.taosd_fqdn[0], ["pip3 install taospyudf"])
        self._remote.cmd(self.taosd_fqdn[0], ["python3 -m pip install --upgrade pip"])
        self._remote.cmd(self.taospy_fqdn[0], ["pip3 install taospyudf"])
        self.table_count = 400
        self.dbname = "DEFAULT_MEIOT_1"
        self.stbname = "t_lingang_stable_sensor_shadow_pressure_1"
        self.current_path = os.path.abspath(__file__)
        self.file_path = os.path.dirname(self.current_path)
        self.operator_int_linear_convert_func = f'{self.file_path}/operator_int_linear_convert_v1.0.py'
        self.operator_int_unsigned_clamping_func = f'{self.file_path}/operator_int_unsigned_clamping_v1.0.py'
        self.udf_get_abspressure_atmos_encoded_func = f'{self.file_path}/udf_get_abspressure_atmos_encoded.py'

        self.total_records = 6000
        self.batch_size = 10000  # Limit per SQL statement
        self.freq_hz = 100
        self.mv_range = (2000, 3000)
        self.super_table = f"{self.dbname}.{self.stbname}"


    def prepare(self):
        """
        Prepare the test environment by creating necessary database objects and functions.
        """
        self.tdCom.createDb(self.dbname)
        self.tdSql.execute(f'create stable if not exists {self.stbname} (ts timestamp, mv int) tags (sensor_type BINARY(20), next_stage BINARY(50),notes BINARY(255),atmos_pressure_bar FLOAT,param_a DOUBLE,param_b DOUBLE)')
        self.tdSql.execute(f'create function if not exists udf_operator_int_linear_convert_v1 as "{self.operator_int_linear_convert_func}" outputtype double language "Python";')
        self.tdSql.execute(f'create function if not exists udf_operator_int_unsigned_clamping_v1 as "{self.operator_int_unsigned_clamping_func}" outputtype double language "Python";')
        self.tdSql.execute(f'create function if not exists udf_enc_atmos_abs_pres as "{self.udf_get_abspressure_atmos_encoded_func}" outputtype double language "Python";')
        self.tdSql.execute(f'CREATE STREAM IF NOT EXISTS t_lingang_pressure_sensor_shadow_to_vsensor_1 TRIGGER AT_ONCE IGNORE UPDATE 0 IGNORE EXPIRED 0 WATERMARK 10d INTO DEFAULT_MEIOT_1.t_lingang_stable_vsensor_cache_pressure_1 (ts, pressure, atmos_pressure) TAGS (dptbname VARCHAR(100)) SUBTABLE(CONCAT("t_lingang_vsp_", dptbname)) AS SELECT _wstart AS ts, FIRST(udf_operator_int_unsigned_clamping_v1(udf_operator_int_linear_convert_v1(udf_operator_int_linear_convert_v1(mv, param_a, param_b),1,udf_operator_int_unsigned_clamping_v1(udf_enc_atmos_abs_pres(atmos_pressure_bar))))) AS pressure, FIRST(udf_operator_int_unsigned_clamping_v1((atmos_pressure_bar))) AS atmos_pressure FROM DEFAULT_MEIOT_1.t_lingang_stable_sensor_shadow_pressure_1 PARTITION BY next_stage dptbname INTERVAL(1a);')
        self.create_subtables()

    def create_subtables(self):
        """
        Creates subtables in the database.

        This method iterates over a range of table counts and creates subtables in the database.
        Each subtable is created with a unique name, sensor type, next stage, notes, atmospheric pressure,
        param_a, and param_b.

        Raises:
            Exception: If there is an error while creating the table.

        Returns:
            None
        """
        try:
            for index in range(0, self.table_count, 1):
                tb_name = f"st_{index}"
                type = index % 30
                sensor_type = f"sensor_type_{type}"
                stage = index % 13
                next_stage = f"next_stage_{stage}"
                notes = f"notes_{index}"
                atmos_pressure_bar = index * 0.1
                param_a = index
                param_b = index * index
                
                sql = f"CREATE TABLE if not exists {self.dbname}.{tb_name} using {self.dbname}.{self.stbname}  tags (\"{sensor_type}\", \"{next_stage}\", \"{notes}\", {atmos_pressure_bar}, {param_a}, {param_b});"
                self.tdSql.execute(sql)
                self._remote._logger.info(f"create table {self.dbname}.{tb_name}")
        except Exception as e:
            self._remote._logger.info(f"Failed to create table: {self.dbname}.{tb_name}: {e}")

    def generate_data_batch(self, start_index, batch_size, start_ts, freq_hz, mv_range):
        """
        Generate a batch of data points.

        Args:
            start_index (int): The starting index of the batch.
            batch_size (int): The number of data points to generate.
            start_ts (int): The starting timestamp.
            freq_hz (int): The frequency in Hz.
            mv_range (tuple): The range of millivolt values.

        Returns:
            list: A list of tuples representing the generated data points, where each tuple contains a timestamp and a millivolt value.
        """
        interval_ms = 1000 // freq_hz
        data = []
        for i in range(batch_size):
            ts = start_ts + (start_index + i) * interval_ms
            mv = random.randint(*mv_range)
            data.append((ts, mv))
        return data

    def insert_data_in_batches(self, table_name, total_records, batch_size, start_ts, freq_hz, mv_range):
        """
        Insert data into the specified table in batches.

        Args:
            table_name (str): The name of the table to insert data into.
            total_records (int): The total number of records to insert.
            batch_size (int): The size of each data batch.
            start_ts (int): The starting timestamp for the data.
            freq_hz (float): The frequency in hertz.
            mv_range (tuple): The range of millivolt values.

        Returns:
            None

        Raises:
            Exception: If there is an error while inserting data.

        """
        try:
            for start_index in range(0, total_records, batch_size):
                data_batch = self.generate_data_batch(start_index, min(batch_size, total_records - start_index),
                                                start_ts, freq_hz, mv_range)
                values = ', '.join([f"('{time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(ts/1000))}.{int(ts%1000):03}', {mv})"
                                    for ts, mv in data_batch])
                sql = f"INSERT INTO {table_name} (ts, mv) VALUES {values};"
                print(sql)
                self.tdSql.execute(sql)
                self._remote._logger.info(f"Inserted batch starting at index {start_index} into {table_name}")
        except Exception as e:
            self._remote._logger.info(f"Failed to insert data into {table_name}: {e}")

    def get_subtables(self, super_table):
        """
        Retrieves the subtables for a given super table.

        Args:
            super_table (str): The name of the super table.

        Returns:
            list: A list of subtable names.

        Raises:
            Exception: If there is an error retrieving the subtables.
        """
        try:
            # Show table tags for super table
            self.tdSql.query(f"SHOW TABLE TAGS FROM {super_table};")
            # Assuming the first column is the subtable name
            subtables = list(map(lambda x: x[0], self.tdSql.query_data))
            return subtables
        except Exception as e:
            print(f"Failed to retrieve subtables for {super_table}: {e}")

    def run(self):
        self.prepare()
        subtables = self.get_subtables(self.super_table)
        for i in range(2):
            # Insert data into all subtables in batches
            for table_name in subtables:
                table = f"{self.dbname}.{table_name}"
                now_ts = int(time.time() * 1000)  # Current timestamp in milliseconds
                start_ts = now_ts - 60000
                self.insert_data_in_batches(table, self.total_records, self.batch_size, start_ts, self.freq_hz, self.mv_range)
            time.sleep(1)

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            BUG-TS-5410
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.Stream