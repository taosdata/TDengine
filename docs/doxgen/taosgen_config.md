//ANCHOR: configuration_instructions
global:
  connection_info:
    host: 127.0.0.1
    port: 6030
    user: root
    password: taosdata

concurrency: 3

jobs:
  # Create database job
  create-database:
    name: Create Database
    needs: []
    steps:
      - name: Create Database
        uses: actions/create-database
        with:
          ......

  # Create super table job
  create-super-table:
    name: Create Super Table
    needs: [create-database]
    steps:
      ......

  # Create second-level child table job
  create-second-child-table:
    name: Create Second Child Table
    needs: [create-super-table]
    steps:
      ......

  # Create minute-level child table job
  create-minute-child-table:
    name: Create Minute Child Table
    needs: [create-super-table]
    steps:
      ......

  # Insert second-level data job
  insert-second-data:
    name: Insert Second-Level Data
    needs: [create-second-child-table]
    steps:
      ......

  # Insert minute-level data job
  insert-minute-data:
    name: Insert Minute-Level Data
    needs: [create-minute-child-table]
    steps:
      ......
//ANCHOR_END: configuration_instructions


//ANCHOR: stmt_v2_write_config
global:
  confirm_prompt: false
  log_dir: log/
  cfg_dir: /etc/taos/

  # Common structure definition
  connection_info: &db_conn
    host: 127.0.0.1
    port: 6030
    user: root
    password: taosdata
    pool:
      enabled: true
      max_size: 10
      min_size: 2
      connection_timeout: 1000

  data_format: &data_format
    format_type: sql

  data_channel: &data_channel
    channel_type: native

  database_info: &db_info
    name: benchdebug
    drop_if_exists: true
    properties: precision 'ms' vgroups 4

  super_table_info: &stb_info
    name: meters
    columns: &columns_info
      - name: current
        type: float
        min: 0
        max: 100
      - name: voltage
        type: int
        min: 200
        max: 240
      - name: phase
        type: float
        min: 0
        max: 360
    tags: &tags_info
      - name: groupid
        type: int
        min: 1
        max: 10
      - name: location
        type: binary(24)

  tbname_generator: &tbname_generator
    prefix: d
    count: 100000
    from: 0

concurrency: 4

jobs:
  # Create database job
  create-database:
    name: Create Database
    needs: []
    steps:
      - name: Create Database
        uses: actions/create-database
        with:
          connection_info: *db_conn
          database_info: *db_info

  # Create super table job
  create-super-table:
    name: Create Super Table
    needs: [create-database]
    steps:
      - name: Create Super Table
        uses: actions/create-super-table
        with:
          connection_info: *db_conn
          database_info: *db_info
          super_table_info: *stb_info

  # Create child table job
  create-second-child-table:
    name: Create Second Child Table
    needs: [create-super-table]
    steps:
      - name: Create Second Child Table
        uses: actions/create-child-table
        with:
          connection_info: *db_conn
          database_info: *db_info
          super_table_info: *stb_info
          child_table_info:
            table_name:
              source_type: generator
              generator: *tbname_generator
            tags:
              source_type: generator
              generator:
                schema: *tags_info
          batch:
            size: 1000
            concurrency: 10

  # Insert data job
  insert-second-data:
    name: Insert Second-Level Data
    needs: [create-second-child-table]
    steps:
      - name: Insert Second-Level Data
        uses: actions/insert-data
        with:
          # source
          source:
            table_name:
              source_type: generator
              generator: *tbname_generator
            columns:
              source_type: generator
              generator:
                schema: *columns_info

                timestamp_strategy:
                  generator:
                    start_timestamp: 1700000000000
                    timestamp_precision : ms
                    timestamp_step: 1

          # target
          target:
            target_type: tdengine
            tdengine:
              connection_info: *db_conn
              database_info: *db_info
              super_table_info: *stb_info

          # control
          control:
            data_format:
              format_type: stmt
              stmt:
                version: v2
            data_channel:
              channel_type: native
            data_generation:
              interlace_mode:
                enabled: true
                rows: 1
              generate_threads: 2
              per_table_rows: 100
              queue_capacity: 100
              queue_warmup_ratio: 0.5
            insert_control:
              per_request_rows: 10000
              auto_create_table: false
              insert_threads: 8
//ANCHOR_END: stmt_v2_write_config

//ANCHOR: csv_stmt_v2_write_config

global:
  confirm_prompt: false
  log_dir: log/
  cfg_dir: /etc/taos/

  # Common structure definition
  connection_info: &db_conn
    host: 127.0.0.1
    port: 6030
    user: root
    password: taosdata
    pool:
      enabled: true
      max_size: 10
      min_size: 2
      connection_timeout: 1000

  data_format: &data_format
    format_type: sql

  data_channel: &data_channel
    channel_type: native

  database_info: &db_info
    name: benchdebug
    drop_if_exists: true
    properties: precision 'ms' vgroups 4

  super_table_info: &stb_info
    name: meters
    columns: &columns_info
      - name: current
        type: float
        min: 0
        max: 100
      - name: voltage
        type: int
        min: 200
        max: 240
      - name: phase
        type: float
        min: 0
        max: 360
    tags: &tags_info
      - name: groupid
        type: int
        min: 1
        max: 10
      - name: location
        type: binary(24)

  tbname_generator: &tbname_generator
    prefix: d
    count: 100000
    from: 0

concurrency: 4

jobs:
  # Create database job
  create-database:
    name: Create Database
    needs: []
    steps:
      - name: Create Database
        uses: actions/create-database
        with:
          connection_info: *db_conn
          database_info: *db_info

  # Create super table job
  create-super-table:
    name: Create Super Table
    needs: [create-database]
    steps:
      - name: Create Super Table
        uses: actions/create-super-table
        with:
          connection_info: *db_conn
          database_info: *db_info
          super_table_info: *stb_info

  # Create child table job
  create-second-child-table:
    name: Create Second Child Table
    needs: [create-super-table]
    steps:
      - name: Create Second Child Table
        uses: actions/create-child-table
        with:
          connection_info: *db_conn
          database_info: *db_info
          super_table_info: *stb_info
          child_table_info:
            table_name:
              source_type: csv
              csv:
                file_path: ../src/parameter/conf/ctb-tags.csv
                tbname_index: 2
            tags:
              source_type: csv
              csv:
                schema: *tags_info
                file_path: ../src/parameter/conf/ctb-tags.csv
                exclude_indices: 2
          batch:
            size: 1000
            concurrency: 10

  # Insert data job
  insert-second-data:
    name: Insert Second-Level Data
    needs: [create-second-child-table]
    steps:
      - name: Insert Second-Level Data
        uses: actions/insert-data
        with:
          # source
          source:
            table_name:
              source_type: csv
              csv:
                file_path: ../src/parameter/conf/ctb-tags.csv
                tbname_index: 2
            columns:
              source_type: csv
              csv:
                schema: *columns_info
                file_path: ../src/parameter/conf/ctb-data.csv
                tbname_index : 0

                timestamp_strategy:
                  strategy_type: generator
                  generator:
                    start_timestamp: 1700000000000
                    timestamp_precision : ms
                    timestamp_step: 1

          # target
          target:
            target_type: tdengine
            tdengine:
              connection_info: *db_conn
              database_info: *db_info
              super_table_info: *stb_info

          # control
          control:
            data_format:
              format_type: stmt
              stmt:
                version: v2
            data_channel:
              channel_type: native
            data_generation:
              interlace_mode:
                enabled: true
                rows: 1
              generate_threads: 1
              per_table_rows: 100
              queue_capacity: 100
              queue_warmup_ratio: 0.0
            insert_control:
              per_request_rows: 10000
              insert_threads: 1
//ANCHOR_END: csv_stmt_v2_write_config


//ANCHOR: write_mqtt_config
global:
  confirm_prompt: false

  super_table_info: &stb_info
    name: meters
    columns: &columns_info
      - name: current
        type: float
        min: 0
        max: 100
      - name: voltage
        type: int
        min: 200
        max: 240
      - name: phase
        type: float
        min: 0
        max: 360
      - name: state
        type: varchar(20)
        values:
          - "normal"
          - "warning"
          - "critical"

  tbname_generator: &tbname_generator
    prefix: d
    count: 10000
    from: 0

concurrency: 1

jobs:
  # Insert data job
  insert-into-mqtt:
    name: Insert Data Into MQTT
    needs: []
    steps:
      - name: Insert Data Into MQTT
        uses: actions/insert-data
        with:
          # source
          source:
            table_name:
              source_type: generator
              generator: *tbname_generator
            columns:
              source_type: generator
              generator:
                schema: *columns_info

                timestamp_strategy:
                  generator:
                    start_timestamp: 1700000000000
                    timestamp_precision : ms
                    timestamp_step: 1

          # target
          target:
            target_type: mqtt
            mqtt:
              host: localhost
              port: 1883
              user: testuser
              password: testpassword
              client_id: mqtt_client
              keep_alive: 60
              clean_session: true
              qos: 1
              topic: factory/{table}/{state}/data

          # control
          control:
            data_format:
              format_type: stmt
              stmt:
                version: v2
            data_channel:
              channel_type: native
            data_generation:
              interlace_mode:
                enabled: true
                rows: 1
              generate_threads: 1
              per_table_rows: 1000
              queue_capacity: 10
              queue_warmup_ratio: 0.00
            insert_control:
              per_request_rows: 10
              insert_threads: 8

//ANCHOR_END: write_mqtt_config
