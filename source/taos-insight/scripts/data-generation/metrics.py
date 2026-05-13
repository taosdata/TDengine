# ==================== Metric Templates ====================

SYS_METRICS = [
    ("sys_cpu_cores", "12f64"),
    ("sys_total_memory", "8000000000f64"),
    ("sys_used_memory", "4000000000f64"),
    ("sys_available_memory", "2000000000f64"),
    ("process_id", "12343f64"),
    ("process_cpu_percent", "30f64"),
    ("process_memory_percent", "30f64"),
    ("process_disk_read_bytes", "6000000000f64"),
    ("process_disk_written_bytes", "6000000000f64"),
    ("process_uptime", "100f64"),
    ("running_tasks", "31f64"),
    ("completed_tasks", "32f64"),
    ("failed_tasks", "33f64"),
]

AGENT_METRICS = [
    ("sys_cpu_cores", "12f64"),
    ("sys_total_memory", "8000000000f64"),
    ("sys_used_memory", "4000000000f64"),
    ("sys_available_memory", "2000000000f64"),
    ("process_uptime", "100f64"),
    ("process_cpu_percent", "30f64"),
    ("process_memory_percent", "30f64"),
    ("process_start_time", "350f64"),
    ("process_id", "12343f64"),
    ("process_disk_read_bytes", "6000000000f64"),
    ("process_disk_written_bytes", "6000000000f64"),
]

CONNECTOR_METRICS = [
    ("process_id", "12343f64"),
    ("process_cpu_percent", "30f64"),
    ("process_memory_percent", "30f64"),
    ("process_disk_read_bytes", "6000000000f64"),
    ("process_disk_written_bytes", "6000000000f64"),
    ("process_uptime", "100f64"),
]

# TDengine2 Legacy Metrics
TDENGINE2_METRICS = [
    ("total_execute_time", "123456f64"),
    ("total_written_rows", "1200f64"),
    ("total_written_points", "60000f64"),
    ("start_time", "12345678f64"),
    ("written_rows", "1234f64"),
    ("written_points", "123400f64"),
    ("execute_time", "12345678f64"),
    ("received_messages", "15000f64"),
    ("processed_messages", "15000f64"),
    ("total_stables", "100f64"),
    ("total_tables", "12345f64"),
    ("total_finished_tables", "1234f64"),
    ("total_success_blocks", "123450f64"),
    ("total_created_tables", "1234f64"),
    ("total_updated_tags", "123f64"),
    ("read_concurrency", "8f64"),
    ("finished_tables", "12345f64"),
    ("success_blocks", "12345f64"),
    ("created_tables", "12345f64"),
    ("updated_tags", "15f64"),
]

# TDengine3 TMQ Metrics
TDENGINE3_METRICS = [
    ("total_execute_time", "123456f64"),
    ("total_written_rows", "1200f64"),
    ("total_written_points", "60000f64"),
    ("start_time", "12345678f64"),
    ("written_rows", "1234f64"),
    ("written_points", "123400f64"),
    ("execute_time", "12345678f64"),
    ("received_messages", "15000f64"),
    ("processed_messages", "15000f64"),
    ("total_messages", "1234567f64"),
    ("total_messages_bytes", "123456789f64"),
    ("total_messages_of_meta", "1234f64"),
    ("total_messages_of_data", "12345f64"),
    ("total_write_raw_fails", "12f64"),
    ("total_success_blocks", "123455f64"),
    ("messages", "12345678f64"),
    ("messages_bytes", "15000f64"),
    ("messages_of_meta", "12345f64"),
    ("messages_of_data", "123456f64"),
    ("write_raw_fails", "15f64"),
    ("success_blocks", "15000f64"),
    ("commits", "1000f64"),
    ("consumers", "123f64"),
    ("topics", "12f64"),
    ("total_consume_cost_ms", "123456f64"),
    ("total_write_cost_ms", "654321f64"),
]

# IPC Common Metrics
IPC_COMMON_METRICS = [
    ("total_execute_time", "123456f64"),
    ("total_written_rows", "1200f64"),
    ("total_written_points", "60000f64"),
    ("start_time", "12345678f64"),
    ("written_rows", "1234f64"),
    ("written_points", "123400f64"),
    ("execute_time", "12345678f64"),
    ("received_messages", "15000f64"),
    ("processed_messages", "15000f64"),
    ("total_received_batches", "1234567f64"),
    ("total_processed_batches", "1234f64"),
    ("total_processed_rows", "12345f64"),
    ("total_inserted_sqls", "12f64"),
    ("total_failed_sqls", "123455f64"),
    ("total_created_stables", "12f64"),
    ("total_created_tables", "123f64"),
    ("total_failed_rows", "12345678f64"),
    ("total_failed_points", "12345f64"),
    ("total_written_blocks", "123456f64"),
    ("total_failed_blocks", "15f64"),
    ("received_batches", "15000f64"),
    ("processed_batches", "15000f64"),
    ("processed_rows", "15000f64"),
    ("received_records", "15000f64"),
    ("inserted_sqls", "15000f64"),
    ("failed_sqls", "15000f64"),
    ("created_stables", "15000f64"),
    ("created_tables", "15000f64"),
    ("failed_rows", "15000f64"),
    ("failed_points", "15000f64"),
    ("written_blocks", "15000f64"),
    ("failed_blocks", "15000f64"),
]

# ==================== Extra Metrics By Data Source ====================

MQTT_EXTRA = [
    ("mqtt_fetched_acks", "15000f64"),
    ("mqtt_ack_fails", "150f64"),
    ("mqtt_discarded_dump_messages", "100f64"),
    ("mqtt_fetched_messages", "15000f64"),
    ("mqtt_dumped_messages", "14000f64"),
    ("mqtt_unprocessed_messages", "500f64"),
    ("mqtt_sent_batches", "1000f64"),
    ("mqtt_discarded_messages", "50f64"),
    ("mqtt_received_bytes", "60000000f64"),
]

SPARKPLUGB_EXTRA = [
    ("sparkplugb_fetched_acks", "15000f64"),
    ("sparkplugb_ack_fails", "150f64"),
    ("sparkplugb_discarded_dump_messages", "100f64"),
    ("sparkplugb_fetched_messages", "15000f64"),
    ("sparkplugb_dumped_messages", "14000f64"),
    ("sparkplugb_unprocessed_messages", "500f64"),
    ("sparkplugb_sent_batches", "1000f64"),
    ("sparkplugb_discarded_messages", "50f64"),
    ("sparkplugb_received_bytes", "60000000f64"),
]

KAFKA_EXTRA = [
    ("kafka_fetched_messages", "15000f64"),
    ("kafka_fetched_bytes", "60000000f64"),
]

PULSAR_EXTRA = [
    ("pulsar_fetched_messages", "15000f64"),
    ("pulsar_fetched_bytes", "60000000f64"),
]

CSV_EXTRA = [
    ("csv_read_bytes", "60000000f64"),
    ("csv_read_lines", "150000f64"),
]

TMQ2MQTT_EXTRA = [
    ("tmq_messages", "15000f64"),
    ("tmq_messages_of_meta", "1000f64"),
    ("tmq_messages_of_data", "14000f64"),
    ("tmq_messages_bytes", "60000000f64"),
    ("mqtt_sent_messages", "14500f64"),
    ("mqtt_sent_bytes", "58000000f64"),
]

PROGRESS_METRICS = [
    ("offset", "1703484900000f64"),
    ("latest", "1703484850000f64"),
]

# ==================== Factory Functions ====================

def make_metrics(metrics_list):
    """Convert a list of (name, value) tuples to metric dictionaries."""
    return [{"name": k, "value": v} for k, v in metrics_list]


def make_sys():
    """Build a taosx_sys record."""
    return {
        "ts": "1703484955810",
        "stable_name": "taosx_sys",
        "tags": [{"name": "taosx_id", "value": "taosx_id_1"}],
        "metrics": make_metrics(SYS_METRICS),
    }


def make_agent(agent_id, agent_name):
    """Build a taosx_agent record."""
    return {
        "ts": "1703484955810",
        "stable_name": "taosx_agent",
        "tags": [
            {"name": "taosx_id", "value": "taosx_id_1"},
            {"name": "agent_id", "value": agent_id},
            {"name": "agent_name", "value": agent_name},
        ],
        "metrics": make_metrics(AGENT_METRICS),
    }


def make_connector(ds_name, task_id, job_id):
    """Build a taosx_connector record."""
    return {
        "ts": "1703484955810",
        "stable_name": "taosx_connector",
        "tags": [
            {"name": "taosx_id", "value": "taosx_id_1"},
            {"name": "ds_name", "value": ds_name},
            {"name": "task_id", "value": task_id},
            {"name": "job_id", "value": job_id},
        ],
        "metrics": make_metrics(CONNECTOR_METRICS),
    }


def make_tdengine2_task(task_id, task_name):
    """Build a taosx_task_tdengine2 record."""
    return {
        "ts": "1703484955810",
        "stable_name": "taosx_task_tdengine2",
        "tags": [
            {"name": "taosx_id", "value": "taosx_id_1"},
            {"name": "task_id", "value": task_id},
            {"name": "task_name", "value": task_name},
        ],
        "metrics": make_metrics(TDENGINE2_METRICS),
    }


def make_tdengine3_task(task_id, task_name):
    """Build a taosx_task_tdengine3 record."""
    return {
        "ts": "1703484955810",
        "stable_name": "taosx_task_tdengine3",
        "tags": [
            {"name": "taosx_id", "value": "taosx_id_1"},
            {"name": "task_id", "value": task_id},
            {"name": "task_name", "value": task_name},
        ],
        "metrics": make_metrics(TDENGINE3_METRICS),
    }


def make_ipc_task(table_name, task_id, task_name, extra_metrics=None):
    """Build an IPC task record."""
    metrics = IPC_COMMON_METRICS.copy()
    if extra_metrics:
        metrics = metrics + extra_metrics
    return {
        "ts": "1703484955810",
        "stable_name": table_name,
        "tags": [
            {"name": "taosx_id", "value": "taosx_id_1"},
            {"name": "task_id", "value": task_id},
            {"name": "task_name", "value": task_name},
        ],
        "metrics": make_metrics(metrics),
    }


def make_progress(task_id, topic, vgroup, offset="1703484900000f64", latest="1703484850000f64"):
    """Build a taosx_task_progress record."""
    return {
        "ts": "1703484955810",
        "stable_name": "taosx_task_progress",
        "tags": [
            {"name": "taosx_id", "value": "taosx_id_1"},
            {"name": "task_id", "value": task_id},
            {"name": "topic", "value": topic},
            {"name": "vgroup", "value": vgroup},
        ],
        "metrics": [
            {"name": "offset", "value": offset},
            {"name": "latest", "value": latest},
        ],
    }


# ==================== Data Configuration ====================

AGENTS = [
    ("agent_id_1", "agent_name_1"),
    ("agent_id_2", "agent_name_2"),
]

CONNECTORS = [
    # (ds_name, task_id, job_id)
    ("tdengine2", "task_id_13", "job_id_13"),
    ("tdengine3", "task_id_1", "job_id_1"),
    ("opc", "task_id_14", "job_id_14"),
    ("opcua", "task_id_2", "job_id_2"),
    ("opcda", "task_id_opcda_1", "job_id_opcda_1"),
    ("pi", "task_id_8", "job_id_8"),
    ("pibackfill", "task_id_9", "job_id_9"),
    ("mqtt", "task_id_4", "job_id_4"),
    ("sparkplugb", "task_id_15", "job_id_15"),
    ("influxdb", "task_id_6", "job_id_6"),
    ("opentsdb", "task_id_7", "job_id_7"),
    ("kafka", "task_id_12", "job_id_12"),
    ("pulsar", "task_id_23", "job_id_23"),
    ("pulsartuya", "task_id_24", "job_id_24"),
    ("csv", "task_id_10", "job_id_10"),
    ("avevahistorian", "task_id_11", "job_id_11"),
    ("mysql", "task_id_16", "job_id_16"),
    ("postgres", "task_id_17", "job_id_17"),
    ("oracle", "task_id_18", "job_id_18"),
    ("mssql", "task_id_19", "job_id_19"),
    ("mongodb", "task_id_20", "job_id_20"),
    ("local", "task_id_21", "job_id_21"),
    ("orc", "task_id_22", "job_id_22"),
    ("kinghist", "task_id_25", "job_id_25"),
    ("tmq", "task_id_26", "job_id_26"),
]

IPC_TASKS = [
    # (table_name, task_id, task_name, extra_metrics)
    ("taosx_task_opc", "task_id_14", "task_name_14", None),
    ("taosx_task_opcua", "task_id_2", "task_name_2", None),
    ("taosx_task_opcua", "task_id_3", "task_name_3", None),
    ("taosx_task_opcda", "task_id_opcda_1", "task_name_opcda_1", None),
    ("taosx_task_pi", "task_id_8", "task_name_8", None),
    ("taosx_task_pibackfill", "task_id_9", "task_name_9", None),
    ("taosx_task_mqtt", "task_id_4", "task_name_4", MQTT_EXTRA),
    ("taosx_task_mqtt", "task_id_5", "task_name_5", MQTT_EXTRA),
    ("taosx_task_sparkplugb", "task_id_15", "task_name_15", SPARKPLUGB_EXTRA),
    ("taosx_task_influxdb", "task_id_6", "task_name_6", None),
    ("taosx_task_opentsdb", "task_id_7", "task_name_7", None),
    ("taosx_task_kafka", "task_id_12", "task_name_12", KAFKA_EXTRA),
    ("taosx_task_pulsar", "task_id_23", "task_name_23", PULSAR_EXTRA),
    ("taosx_task_pulsartuya", "task_id_24", "task_name_24", PULSAR_EXTRA),
    ("taosx_task_csv", "task_id_10", "task_name_10", CSV_EXTRA),
    ("taosx_task_avevahistorian", "task_id_11", "task_name_11", None),
    ("taosx_task_mysql", "task_id_16", "task_name_16", None),
    ("taosx_task_postgres", "task_id_17", "task_name_17", None),
    ("taosx_task_oracle", "task_id_18", "task_name_18", None),
    ("taosx_task_mssql", "task_id_19", "task_name_19", None),
    ("taosx_task_mongodb", "task_id_20", "task_name_20", None),
    ("taosx_task_local", "task_id_21", "task_name_21", None),
    ("taosx_task_orc", "task_id_22", "task_name_22", None),
    ("taosx_task_kinghist", "task_id_25", "task_name_25", None),
    ("taosx_task_tmq", "task_id_26", "task_name_26", TMQ2MQTT_EXTRA),
]

PROGRESS_DATA = [
    # (task_id, topic, vgroup, offset, latest)
    ("task_id_1", "topic_1", "1", "1703484900000f64", "1703484850000f64"),
    ("task_id_1", "topic_1", "2", "1703484910000f64", "1703484860000f64"),
    ("task_id_1", "topic_2", "1", "1703484920000f64", "1703484870000f64"),
    ("task_id_1", "topic_2", "2", "1703484930000f64", "1703484880000f64"),
]

# ==================== Build all_metrics ====================

all_metrics = [
    # taosx_sys
    make_sys(),
    # taosx_agent
    *[make_agent(aid, aname) for aid, aname in AGENTS],
    # taosx_connector
    *[make_connector(ds, tid, jid) for ds, tid, jid in CONNECTORS],
    # taosx_task_tdengine2
    make_tdengine2_task("task_id_13", "task_name_13"),
    # taosx_task_tdengine3
    make_tdengine3_task("task_id_1", "task_name_1"),
    # taosx_task_progress
    *[make_progress(tid, topic, vg, offset, latest) for tid, topic, vg, offset, latest in PROGRESS_DATA],
    # IPC tasks
    *[make_ipc_task(tbl, tid, tname, extra) for tbl, tid, tname, extra in IPC_TASKS],
]
