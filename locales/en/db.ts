export default {
  edit: 'Edit Database',
  create: 'Create Database',
  nameExisted: 'Database name "{0}" already exists',
  backslashTip:
    'If you want to create a database with a case-sensitive name, please add ` before and after the name. For example, `testDB`.',
  replica: 'Replica',
  bufferTip:
    'specifies the size (in MB) of the write buffer for each vnode. Enter a value between 3 and 16384. The default value is 32.',
  cacheModelTip: `specifies how the latest data in subtables is stored in the cache. The default value is none.<ul><li>none: The latest data is not cached.</li><li>last_row: The last row of each subtable is cached. This option significantly improves the performance of the LAST_ROW function.</li><li>last_value: The last non-null value of each column in each subtable is cached. This option significantly improves the performance of the LAST function under normal circumstances, such as statements including the WHERE, ORDER BY, GROUP BY, and INTERVAL keywords.</li><li>both: The last row of each subtable and the last non-null value of each column in each subtable are cached.</li></ul>`,
  compTip: ` specifies how databases are compressed. The default value is 2.<ul><li>0: Compression is disabled.</li><li>1: One-pass compression is enabled.</li><li>2: Two-pass compression is enabled.</li></ul>`,
  durationTip:
    'specifies the time period contained in each data file. After the time specified by this parameter has elapsed, TDengine creates a new data file to store incoming data. You can use m (minutes), h (hours), and d (days) as the unit, for example DURATION 100h or DURATION 10d. If you do not include a unit, d is used by default.',
  walFsyncPeriodTip:
    'specifies the interval (in milliseconds) at which data is written from the WAL to disk. This parameter takes effect only when the WAL parameter is set to 2. The default value is 3000. Enter a value between 0 and 180000. The value 0 indicates that incoming data is immediately written to disk.',
  cacheSizeTip:
    'specifies the amount (in MB) of memory used for subtable caching on each vnode. Enter a value between 1 and 65536. The default value is 1.',
  maxRowsTip: 'specifies the maximum number of rows recorded in a block. The default value is 4096.',
  minRowsTip: 'specifies the minimum number of rows recorded in a block. The default value is 100.',
  keepTip:
    'specifies the time for which data is retained. Enter a value between 1 and 365000. The default value is 3650. The value of the KEEP parameter must be greater than or equal to the value of the DURATION parameter. TDengine automatically deletes data that is older than the value of the KEEP parameter. You can use m (minutes), h (hours), and d (days) as the unit, for example KEEP 100h or KEEP 10d. If you do not include a unit, d is used by default. TDengine Enterprise supports Tiered Storage function, thus multiple KEEP values (comma separated and up to 3 values supported, and meet keep 0 <= keep 1 <= keep 2, e.g. KEEP 100h,100d,3650d) are supported; TDengine OSS does not support Tiered Storage function (although multiple keep values are configured, they do not take effect, only the maximum keep value is used as KEEP).',
  pagesTip:
    'specifies the number of pages in the metadata storage engine cache on each vnode. Enter a value greater than or equal to 64. The default value is 256. The space occupied by metadata storage on each vnode is equal to the product of the values of the PAGESIZE and PAGES parameters. The space occupied by default is 1 MB.',
  pageSizeTip:
    'specifies the size (in KB) of each page in the metadata storage engine cache on each vnode. The default value is 4. Enter a value between 1 and 16384.',
  precisionTip:
    'specifies the precision at which a database records timestamps. Enter ms for milliseconds, us for microseconds, or ns for nanoseconds. The default value is ms.',
  replicaTip:
    'specifies the number of replicas that are made of the database. Enter 1 or 3. The default value is {0}. The value of the REPLICA parameter cannot exceed the number of dnodes in the cluster.',
  retentionsTip:
    'specifies the retention period for data aggregated at various intervals. For example, RETENTIONS 15s:7d,1m:21d,15m:50d indicates that data aggregated every 15 seconds is retained for 7 days, data aggregated every 1 minute is retained for 21 days, and data aggregated every 15 minutes is retained for 50 days. You must enter three aggregation intervals and corresponding retention periods.',
  strictTip: `specifies whether strong data consistency is enabled. The default value is off.
  <ul><li>on: Strong consistency is enabled and implemented through the Raft consensus algorithm. In this mode, an operation is considered successful once it is confirmed by half of the nodes in the cluster.</li><li>off: Strong consistency is disabled. In this mode, an operation is considered successful when it is initiated by the local node.</li></ul> `,
  walLevelTip: `specifies whether fsync is enabled. The default value is 1.<ul><li>1: WAL is enabled but fsync is disabled.</li><li>2: WAL and fsync are both enabled.</li></ul>`,
  vgroupsTip: 'specifies the initial number of vgroups when a database is created.',
  singleStableTip: `specifies whether the database can contain more than one supertable.<ul><li>0: The database can contain multiple supertables.</li><li>1: The database can contain only one supertable.</li></ul>`,
  sttTaiggerTip:
    'specifies the number of file merges triggered by flushed files. The default is 8, ranging from 1 to 16. For high-frequency scenarios with few tables, it is recommended to use the default configuration or a smaller value for this parameter; For multi-table low-frequency scenarios, it is recommended to configure this parameter with a larger value.',
  tablePrefixTip:
    'The prefix in the table name that is ignored when distributing a table to a vgroup when it\'s a positive number, or only the prefix is used when distributing a table to a vgroup, the default value is 0; For example, if the table name v30001, then "0001" is used if TSDB_PREFIX is set to 2 but "v3" is used if TSDB_PREFIX is set to -2; It can help you to control the distribution of tables.',
  tableSuffixTip:
    'The suffix in the table name that is ignored when distributing a table to a vgroup when it\'s a positive number, or only the suffix is used when distributing a table to a vgroup, the default value is 0; For example, if the table name v30001, then "v300" is used if TSDB_SUFFIX is set to 2 but "01" is used if TSDB_SUFFIX is set to -2; It can help you to control the distribution of tables.',
  tsdbPagesizeTip:
    'The page size of the data storage engine in a vnode. The unit is KB. The default is 4 KB. The range is 1 to 16384, that is, 1 KB to 16 MB.',
  walRetentionPeriodTip: `specifies the maximum time of which WAL files are to be kept for consumption. This parameter is used for data subscription. Enter a time in seconds. The default value is 3600, which means the data in latest 3600 seconds will be kept in WAL for data subscription. Please adjust this parameter to a more proper value for your data subscription.\n If you need to use the backup function, the setting of this parameter must be greater than the backup period + 24 hours.`,
  walRetentionSizeTip:
    'specifies the maximum total size of which WAL files are to be kept for consumption. This parameter is used for data subscription. Enter a size in KB. The default value is 0. A value of 0 indicates that the total size of WAL files to keep for consumption has no upper limit.',
  walRollPeriodTip: `specifies the time after which WAL files are rotated. After this period elapses, a new WAL file is created. The default value is 0. A value of 0 indicates that a new WAL file is created only after TSDB data in memory are flushed to disk.`,
  walSegmentSizeTip: `specifies the maximum size of a WAL file. After the current WAL file reaches this size, a new WAL file is created. The default value is 0. A value of 0 indicates that a new WAL file is created only after TSDB data in memory are flushed to disk.`,
  replica1Tip:
    'The current instance is a three nodes high availability instance, but your database is a single replica without high availability, please pay attention to create it.',
  performanceRelatedParameters: 'Performance Related Parameters',
  dataPersistenceParameters: 'Data Persistence Parameters',
  s3KeepLocalTip:
    'The number of days the data is kept locally, that is, how long the data file can be uploaded to S3 after it is kept on the local disk. The default value is 365 days, the value range is [1,365000], and it must be greater than or equal to 3 times the value of the duration parameter. You can use the plus unit representation, such as S3_KEEPLOCAL 100h, S3_KEEPLOCAL 10d, etc., and support three units of m (minutes), h (hours), and d (days).',
  s3ChunkPagesTip:
    'The size threshold of the upload object, and the tsdb_pagesize parameters cannot be modified, the unit is TSDB page, and can only be configured as a number; the default value is 262144 pages, the minimum value is 131072 pages, and the maximum value is 1048576 pages.',
  s3CompactTip:
    'When uploading S3 for the first time, whether to compact the file group. When triggering the s3 data upload process, it will first check whether the data file has been compact. If not and the s3_compact parameter value is 1, the compact operation of this file group will be initiated. The next time the s3 data upload is triggered, the upload operation will be carried out directly.',
  encryptTip:
    'Specifies the encryption algorithm used for the data. The default is none, that is, no encryption is used. Sm4 means that the sm4 encryption algorithm is used. Currently only the sm4 encryption algorithm is supported. If you enter other unsupported encryption algorithms, an error will be reported and the library will fail.',
  walParameters: 'WAL Parameters',
  specialParameters: 'Special Parameters',
  delete: 'Delete Database',
  viewDatabase: 'View Database Config',
  managePrivilege: 'Manage Database Privilege',
  operationLog: 'Operation Log',
  source: 'Source DB',
  target: 'Target DB'
};
