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

import time
import threading
import signal
import subprocess
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from taostest.util.common import TDCom
from taostest import TDCase, T
from taostest.components.taosd import TaosD
from taostest.util.remote import Remote
from taosws import Consumer

class TestTs6903(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.taosd = TaosD(self._remote)
        self.taosd_setting = self.tdCom.get_components_setting(
            self.env_setting["settings"], "taosd"
        )
        self.taosadapter_setting = self.tdCom.get_components_setting(
            self.env_setting["settings"], "taosadapter"
        )
        self.taosadapter_fqdn_list = self.taosadapter_setting["fqdn"]
        
        # Test configuration
        self.dbname = "test_sub_db"
        self.stbname = "meters"
        self.replica = 1
        self.vgroups = 1
        self.childtable_count = 10000
        self.records_per_second_per_table = 10
        self.total_records_per_second = 100000
        self.topic_count = 10
        self.groups_per_topic = 10
        self.consumers_per_group = 1
        
        # Consumer configuration
        self.consumer_ip = self.taosd_setting["spec"]["config"]["firstEP"].split(":")[0]
        self.consumer_port = "6041"
        self.consumer_connect_scheme = "ws"
        
        # Test control
        self.stop_flag = False
        self.insert_threads = []
        self.consumer_threads = []
        self.benchmark_process = None  # Store taosBenchmark process
        self.start_polling_flag = False  # Control when to start polling
        
        # Column and tag definitions
        self.column_info_list = [
            {"type": "FLOAT", "count": 1},
            {"type": "INT", "count": 1},
            {"type": "BINARY", "len": 16, "count": 1}
        ]
        self.tag_info_list = [
            {"type": "BINARY", "len": 32, "count": 1},
            {"type": "INT", "count": 1}
        ]

    def desc(self):
        return "TDengine subscription test with 1 DB, 1 vgroup, 1 supertable, 10k subtables, 10 topics, 100 consumers"

    def author(self):
        return "Test Framework"

    def tags(self):
        return T.Write

    def cleanup(self):
        self.stop_flag = True
        
        # Stop taosBenchmark process
        if self.benchmark_process and self.benchmark_process.poll() is None:
            print("Stopping taosBenchmark process...")
            self.benchmark_process.terminate()
            try:
                self.benchmark_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.benchmark_process.kill()
                self.benchmark_process.wait()
        
        # Stop threads
        for thread in self.insert_threads + self.consumer_threads:
            if thread.is_alive():
                thread.join(timeout=5)

    def prepare_database_connection(self):
        """Ensure database connection is ready for taosBenchmark"""
        # Database and tables will be created by taosBenchmark
        pass

    def start_data_insertion(self):
        """Start taosBenchmark for continuous data insertion"""
        import json
        
        # Create taosBenchmark configuration
        taos_benchmark_config = {
            "filetype": "insert",
            "cfgdir": "/etc/taos",
            "host": self.consumer_ip,
            "port": 6030,
            "user": "root",
            "password": "taosdata",
            "thread_count": 100,
            "create_table_thread_count": 100,
            "confirm_parameter_prompt": "no",
            "num_of_records_per_req": 10000,
            "databases": [
                {
                    "dbinfo": {
                        "name": self.dbname,
                        "drop": "yes",
                        "replica": self.replica,
                        "vgroups": 1
                    },
                    "super_tables": [
                        {
                            "name": self.stbname,
                            "child_table_exists": "no",
                            "childtable_count": self.childtable_count,
                            "childtable_prefix": "d",
                            "auto_create_table": "no",
                            "escape_character": "no",
                            "batch_create_tbl_num": 100,
                            "data_source": "rand",
                            "insert_mode": "taosc",
                            "insert_rows": 100000000000,  # Very large number for continuous insert
                            "childtable_limit": self.childtable_count,
                            "interlace_rows": 10,
                            "insert_interval": 1000,  # 1000ms interval
                            "keep_trying": -1,
                            "timestamp_step": 1,
                            "max_sql_len": 1048576,
                            "disorder_ratio": 0,
                            "disorder_range": 1000,
                            "start_timestamp": "now",
                            "columns": [
                                {
                                    "type": "FLOAT",
                                    "name": "current",
                                    "min": 200.0,
                                    "max": 250.0
                                },
                                {
                                    "type": "INT", 
                                    "name": "voltage",
                                    "min": 360,
                                    "max": 400
                                },
                                {
                                    "type": "BINARY",
                                    "name": "phase",
                                    "len": 16,
                                    "values": ["phase_0", "phase_1", "phase_2"]
                                }
                            ],
                            "tags": [
                                {
                                    "type": "BINARY",
                                    "name": "location", 
                                    "len": 32
                                },
                                {
                                    "type": "INT",
                                    "name": "groupid"
                                }
                            ]
                        }
                    ]
                }
            ]
        }
        
        # Save configuration to file
        config_file = f"/tmp/taos_benchmark_{self.dbname}.json"
        with open(config_file, 'w') as f:
            json.dump(taos_benchmark_config, f, indent=2)
        
        print(f"Starting taosBenchmark with config: {config_file}")
        
        def run_taos_benchmark():
            try:
                cmd = f"taosBenchmark -f {config_file}"
                print(f"Executing: {cmd}")
                
                # Start taosBenchmark process with non-blocking output handling
                with open(f"/tmp/taosBenchmark_{self.dbname}.log", "w") as log_file:
                    process = subprocess.Popen(
                        cmd.split(),
                        stdout=log_file,
                        stderr=subprocess.STDOUT,  # Redirect stderr to stdout
                        text=True,
                        bufsize=1,  # Line buffered
                        preexec_fn=None
                    )
                    self.benchmark_process = process
                    print(f"taosBenchmark started with PID: {process.pid}, log file: /tmp/taosBenchmark_{self.dbname}.log")
                
                # Monitor process status without blocking on output
                restart_count = 0
                max_restarts = 3
                
                while not self.stop_flag:
                    if process.poll() is not None:
                        # Process has terminated
                        print(f"taosBenchmark process (PID: {process.pid}) terminated with return code: {process.returncode}")
                        
                        if not self.stop_flag and restart_count < max_restarts:
                            restart_count += 1
                            print(f"Restarting taosBenchmark (attempt {restart_count}/{max_restarts})...")
                            time.sleep(2)  # Wait before restart
                            
                            # Start new process
                            with open(f"/tmp/taosBenchmark_{self.dbname}_{restart_count}.log", "w") as log_file:
                                process = subprocess.Popen(
                                    cmd.split(),
                                    stdout=log_file,
                                    stderr=subprocess.STDOUT,
                                    text=True,
                                    bufsize=1,
                                    preexec_fn=None
                                )
                                self.benchmark_process = process
                                print(f"taosBenchmark restarted with PID: {process.pid}")
                        else:
                            break
                    
                    # Check process status every 5 seconds
                    time.sleep(5)
                    
                    # Log process status periodically
                    if hasattr(process, 'pid'):
                        try:
                            # Check if process is still alive
                            import os
                            os.kill(process.pid, 0)  # Send signal 0 to check if process exists
                            print(f"taosBenchmark process (PID: {process.pid}) is still running")
                        except OSError:
                            print(f"taosBenchmark process (PID: {process.pid}) is not running")
                
                # Cleanup
                if process.poll() is None:
                    print(f"Terminating taosBenchmark process (PID: {process.pid})")
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        print(f"Force killing taosBenchmark process (PID: {process.pid})")
                        process.kill()
                        process.wait()
                    
            except Exception as e:
                print(f"taosBenchmark error: {e}")
                import traceback
                traceback.print_exc()
        
        # Start taosBenchmark in a separate thread
        benchmark_thread = threading.Thread(target=run_taos_benchmark, daemon=True)
        self.insert_threads.append(benchmark_thread)
        benchmark_thread.start()

    def create_topics(self):
        """Create 10 topics at database level"""
        for i in range(self.topic_count):
            topic_name = f"topic_{i}"
            self.tdSql.execute(f'DROP TOPIC IF EXISTS {topic_name}')
            self.tdSql.execute(f'CREATE TOPIC {topic_name} AS DATABASE {self.dbname}')

    def create_consumer_with_timeout(self, consumer_dict, timeout=30):
        """Create consumer with timeout"""
        def create_consumer():
            return Consumer(consumer_dict)
        
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(create_consumer)
            try:
                return future.result(timeout=timeout)
            except TimeoutError:
                self._remote._logger.error(f"Consumer creation timed out after {timeout} seconds")
                return None

    def subscribe_with_timeout(self, consumer, topics, timeout=30):
        """Subscribe to topics with timeout"""
        def subscribe():
            consumer.subscribe(topics)
            return True
        
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(subscribe)
            try:
                return future.result(timeout=timeout)
            except TimeoutError:
                self._remote._logger.error(f"Consumer subscribe timed out after {timeout} seconds")
                return False

    def poll_with_timeout(self, consumer, timeout_ms=1000, operation_timeout=10):
        """Poll with timeout to prevent blocking"""
        def poll():
            return consumer.poll(timeout_ms)
        
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(poll)
            try:
                return future.result(timeout=operation_timeout)
            except TimeoutError:
                print(f"Consumer poll operation timed out after {operation_timeout} seconds")
                return None

    def consumer_worker(self, topic_name, group_id, consumer_id):
        """Worker thread for a single consumer - this will run polling in background"""
        consumer_dict = {
            "td.connect.websocket.scheme": self.consumer_connect_scheme,
            "td.connect.ip": self.consumer_ip,
            "td.connect.port": self.consumer_port,
            "group.id": f"group_{topic_name}_{group_id}",
            "client.id": f"consumer_{topic_name}_{group_id}_{consumer_id}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "true",
            "auto.commit.interval.ms": "1000"
        }
        
        consumer = None
        retry_count = 0
        max_retries = 3
        
        # Create and subscribe consumer
        while retry_count < max_retries and not self.stop_flag:
            try:
                print(f"Creating consumer for topic {topic_name}, group {group_id}, consumer {consumer_id} (attempt {retry_count + 1})")
                
                consumer = self.create_consumer_with_timeout(consumer_dict, 30)
                if consumer is None:
                    raise Exception("Consumer creation timed out")
                
                print(f"Consumer created, now subscribing to topic {topic_name}")
                if not self.subscribe_with_timeout(consumer, [topic_name], 30):
                    raise Exception("Consumer subscribe timed out")
                
                # print(f"Consumer {topic_name}_group{group_id}_consumer{consumer_id} created and subscribed successfully")
                break
                
            except Exception as e:
                retry_count += 1
                print(f"Failed to create consumer for topic {topic_name}, group {group_id}, consumer {consumer_id} (attempt {retry_count}): {e}")
                if consumer:
                    try:
                        consumer.close()
                    except:
                        pass
                    consumer = None
                
                if retry_count < max_retries:
                    time.sleep(5)
                else:
                    print(f"Max retries reached for consumer {consumer_id} in topic {topic_name} group {group_id}")
                    return
        
        if consumer is None or self.stop_flag:
            print(f"Consumer creation failed or stopped for consumer {consumer_id}")
            return
            
        # Add small random delay before starting polling to stagger consumer starts
        # Create unique consumer identifier for logging
        consumer_full_id = f"{topic_name}_group{group_id}_consumer{consumer_id}"
        
        # Wait for permission to start polling
        print(f"Consumer {consumer_full_id} created successfully")
        while not self.start_polling_flag and not self.stop_flag:
            time.sleep(0.1)
        
        if self.stop_flag:
            print(f"Consumer {consumer_full_id} stopping before polling started")
            if consumer:
                consumer.close()
            return
        
        # Start polling loop
        try:
            print(f"Consumer {consumer_full_id} starting polling")
            
            poll_count = 0
            while not self.stop_flag:
                poll_count += 1
                
                try:
                    # Use very short polling timeout to avoid blocking
                    res = consumer.poll(10)  # Only 10ms timeout
                    if res:
                        for block in res:
                            nrows = block.nrows()
                            if nrows > 0:
                                print(f"Consumer {consumer_full_id} consumed {nrows} records (poll #{poll_count})")
                except Exception as poll_error:
                    if poll_count % 1000 == 0:  # Log poll errors less frequently
                        print(f"Consumer {consumer_full_id} poll error: {poll_error}")
                
                # Add longer sleep to reduce system load
                time.sleep(0.5)  # Sleep 500ms between polls
                
                # Log status less frequently
                if poll_count % 20 == 0:  # Every 20 polls (about every 10 seconds)
                    print(f"Consumer {consumer_full_id} polling (poll #{poll_count})")
                
        except Exception as e:
            print(f"Consumer {consumer_full_id} polling error: {e}")
        finally:
            if consumer:
                try:
                    consumer.close()
                    print(f"Consumer {consumer_full_id} closed")
                except Exception as e:
                    print(f"Error closing consumer {consumer_full_id}: {e}")

    def start_consumers(self):
        """Start consumers for all topics and groups - create all threads first, then start them"""
        total_consumers = self.topic_count * self.groups_per_topic * self.consumers_per_group
        print(f"Creating {total_consumers} consumer threads...")
        
        consumer_count = 0
        
        # Step 1: Create all thread objects first
        for topic_idx in range(self.topic_count):
            topic_name = f"topic_{topic_idx}"
            
            for group_idx in range(self.groups_per_topic):
                for consumer_idx in range(self.consumers_per_group):
                    consumer_count += 1
                    print(f"Creating thread object {consumer_count}/{total_consumers}: topic {topic_name} group {group_idx} consumer {consumer_idx}")
                    
                    thread = threading.Thread(
                        target=self.consumer_worker,
                        args=(topic_name, group_idx, consumer_idx),
                        daemon=True
                    )
                    self.consumer_threads.append(thread)
                    
                    if self.stop_flag:
                        print("Stop flag set, breaking thread creation")
                        return
        
        print(f"All {total_consumers} thread objects created, now starting them...")
        
        # Step 2: Start all threads with timeout and error handling
        for i, thread in enumerate(self.consumer_threads):
            print(f"Starting thread {i+1}/{len(self.consumer_threads)}")
            try:
                thread.start()
                print(f"Thread {i+1} started successfully")
                
            except Exception as e:
                print(f"Failed to start thread {i+1}: {e}")
            
            if self.stop_flag:
                print("Stop flag set, breaking thread starting")
                return
        
        print(f"Finished attempting to start all {len(self.consumer_threads)} consumer threads")
        
        # Wait a moment for all consumers to be ready
        print("Waiting 5 seconds for all consumers to be ready...")
        time.sleep(5)
        
        # Now allow all consumers to start polling
        print("Giving permission for all consumers to start polling...")
        self.start_polling_flag = True

    def run(self):
        """Main test execution"""
        self._remote._logger.info("Starting TDengine subscription test")
        
        # Prepare for database operations
        self.prepare_database_connection()
        self._remote._logger.info(f"Preparing to create database {self.dbname} with {self.childtable_count} subtables via taosBenchmark")
        
        # Start data insertion FIRST to ensure continuous data flow
        self.start_data_insertion()
        self._remote._logger.info(f"Started data insertion with {len(self.insert_threads)} threads")
        
        # Wait a moment for some data to be inserted
        print("Waiting 10 seconds for initial data insertion...")
        time.sleep(10)
        
        # Create topics AFTER data insertion has started
        self.create_topics()
        self._remote._logger.info(f"Created {self.topic_count} topics")
        
        # Start consumers LAST to consume the existing and new data
        self.start_consumers()
        self._remote._logger.info(f"Started {len(self.consumer_threads)} consumers")
        
        # Run for a specified duration
        test_duration = 10800  # 3 hour
        self._remote._logger.info(f"Running test for {test_duration} seconds")
        
        start_time = time.time()
        while time.time() - start_time < test_duration:
            if self.stop_flag:
                break
            time.sleep(10)
            
            # Check system status
            alive_insert_threads = sum(1 for t in self.insert_threads if t.is_alive())
            alive_consumer_threads = sum(1 for t in self.consumer_threads if t.is_alive())
            
            self._remote._logger.info(f"Status: {alive_insert_threads} insert threads, {alive_consumer_threads} consumer threads active")
        
        # Stop all operations
        self.stop_flag = True
        self._remote._logger.info("Test completed successfully")