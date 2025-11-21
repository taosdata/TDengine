//ANCHOR: tdengine_gen_stmt_insert_config
tdengine:
  dsn: taos+ws://root:taosdata@127.0.0.1:6041/tsbench
  drop_if_exists: true
  props: precision 'ms' vgroups 4

schema:
  name: meters
  tbname:
    prefix: d
    count: 10000
    from: 0
  columns:
    - name: ts
      type: timestamp
      start: 1700000000000
      precision : ms
      step: 300s
    - name: current
      type: float
      min: 0
      max: 100
    - name: voltage
      type: int
      expr: 220 * math.sqrt(2) * math.sin(_i)
    - name: phase
      type: float
      min: 0
      max: 360
  tags:
    - name: groupid
      type: int
      min: 1
      max: 10
    - name: location
      type: binary(24)
      values:
        - New York
        - Los Angeles
        - Chicago
        - Houston
        - Phoenix
        - Philadelphia
        - San Antonio
        - San Diego
        - Dallas
        - Austin
  generation:
    interlace: 1
    rows_per_table: 10000
    rows_per_batch: 10000
    num_cached_batches: 0

jobs:
  # TDengine insert job
  insert-data:
    steps:
      - uses: tdengine/create-super-table
      - uses: tdengine/create-child-table
        with:
          batch:
            size: 1000
            concurrency: 10

      - uses: tdengine/insert-data
        with:
          concurrency: 8
//ANCHOR_END: tdengine_gen_stmt_insert_config

//ANCHOR: tdengine_gen_stmt_insert_simple
schema:
  columns:
    - name: ts
      type: timestamp
      start: 1700000000000
      precision : ms
      step: 300s
    - name: current
      type: float
      min: 0
      max: 100
    - name: voltage
      type: int
      expr: 220 * math.sqrt(2) * math.sin(_i)
    - name: phase
      type: float
      min: 0
      max: 360
  generation:
    interlace: 1
    num_cached_batches: 0

jobs:
  # TDengine insert job
  insert-data:
    steps:
      - uses: tdengine/create-super-table
      - uses: tdengine/create-child-table
      - uses: tdengine/insert-data
//ANCHOR_END: tdengine_gen_stmt_insert_simple

//ANCHOR: tdengine_csv_stmt_insert_config
tdengine:
  dsn: taos+ws://root:taosdata@127.0.0.1:6041/tsbench
  drop_if_exists: true
  props: precision 'ms' vgroups 4

schema:
  name: meters
  from_csv:
    tags:
      file_path: ./conf/ctb-tags.csv
      tbname_index: 2
      exclude_indices:
    columns:
      file_path: ./conf/ctb-data.csv
      tbname_index : 0
      timestamp_index: 1
      timestamp_precision: ms
      timestamp_offset:
        offset_type: relative
        value: +10s
      repeat_read: false
  columns:
    - name: ts
      type: timestamp
      precision : ms
    - name: current
      type: float
    - name: voltage
      type: int
    - name: phase
      type: float
  tags:
    - name: groupid
      type: int
    - name: location
      type: binary(24)
  generation:
    interlace: 1
    rows_per_table: 10000
    rows_per_batch: 10000
    num_cached_batches: 0
    tables_reuse_data: false

jobs:
  # TDengine insert job
  insert-data:
    steps:
      - uses: tdengine/create-super-table
      - uses: tdengine/create-child-table
        with:
          batch:
            size: 1000
            concurrency: 10
      - uses: tdengine/insert-data
        with:
          concurrency: 8
//ANCHOR_END: tdengine_csv_stmt_insert_config

//ANCHOR: mqtt_publish_config
mqtt:
  uri: tcp://localhost:1883
  user: root
  password: taosdata
  topic: factory/{table}/{location}
  qos: 0

schema:
  name: meters
  tbname:
    prefix: d
    count: 10000
    from: 0
  columns:
    - name: ts
      type: timestamp
      start: 1700000000000
      precision : ms
      step: 300s
    - name: current
      type: float
      min: 0
      max: 100
    - name: voltage
      type: int
      expr: 220 * math.sqrt(2) * math.sin(_i)
    - name: phase
      type: float
      min: 0
      max: 360
    - name: location
      type: varchar(20)
      values:
        - Chicago
        - Houston
        - Phoenix
        - Philadelphia
        - Dallas
        - Austin
  generation:
    interlace: 1
    concurrency: 8
    rows_per_table: 10000
    rows_per_batch: 1000
    num_cached_batches: 0

jobs:
  # MQTT publish job
  publish-data:
    steps:
      - uses: mqtt/publish-data
        with:
          concurrency: 8
//ANCHOR_END: mqtt_publish_config