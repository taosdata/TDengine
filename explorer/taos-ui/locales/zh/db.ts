export default {
  edit: '编辑数据库',
  create: '创建数据库',
  nameExisted: '数据库名称“{0}”已存在',
  backslashTip: '数据库名称是大小写敏感的，比如，testDB 和 testdb 是两个不同的数据库名称',
  replica: '副本数',
  bufferTip: '一个 VNODE 写入内存池大小，单位为 MB，默认为 32，最小为 3，最大为 16384。',
  cacheModelTip: `表示是否在内存中缓存子表的最近数据。默认为 none。<ul><li>none：表示不缓存。</li><li>last_row：表示缓存子表最近一行数据。这将显著改善 LAST_ROW 函数的性能表现。</li><li>last_value：表示缓存子表每一列的最近的非 NULL 值。这将显著改善无特殊影响（WHERE、ORDER BY、GROUP BY、INTERVAL）下的 LAST 函数的性能表现。</li><li>both：表示同时打开缓存最近行和列功能。</li></ul>`,
  compTip: `表示数据库文件压缩标志位，缺省值为 2，取值范围为 [0, 2]。\n    <ul>\n    <li>0：表示不压缩。</li>\n    <li>1：表示一阶段压缩。</li>\n    <li>2：表示两阶段压缩。</li>\n    </ul>\n    `,
  durationTip:
    '数据文件存储数据的时间跨度。可以使用加单位的表示形式，如 DURATION 100h、DURATION 10d 等，支持 m（分钟）、h（小时）和 d（天）三个单位。不加时间单位时默认单位为天，如 DURATION 50 表示 50 天。',
  walFsyncPeriodTip:
    '当 WAL 参数设置为 2 时，落盘的周期。默认为 3000，单位毫秒。最小为 0，表示每次写入立即落盘；最大为 180000，即三分钟。',
  cacheSizeTip: '表示每个 vnode 中用于缓存子表最近数据的内存大小。默认为 1 ，范围是[1, 65536]，单位是 MB。',
  maxRowsTip: '文件块中记录的最大条数，默认为 4096 条。',
  minRowsTip: '文件块中记录的最小条数，默认为 100 条。',
  keepTip:
    '表示数据文件保存的天数，缺省值为 3650，取值范围 [1, 365000]，且必须大于或等于 DURATION 参数值。数据库会自动删除保存时间超过 KEEP 值的数据。KEEP 可以使用加单位的表示形式，如 KEEP 100h、KEEP 10d 等，支持 m（分钟）、h（小时）和 d（天）三个单位。也可以不写单位，如 KEEP 50，此时默认单位为天。企业版支持多级存储功能, 因此, 可以设置多个保存时间（多个以英文逗号分隔，最多 3 个，满足 keep 0 <= keep 1 <= keep 2，如 KEEP 100h,100d,3650d）; 社区版不支持多级存储功能（即使配置了多个保存时间, 也不会生效, KEEP 会取最大的保存时间）',
  pagesTip:
    '一个 VNODE 中元数据存储引擎的缓存页个数，默认为 256，最小 64。一个 VNODE 元数据存储占用 PAGESIZE * PAGES，默认情况下为 1MB 内存。',
  pageSizeTip: '一个 VNODE 中元数据存储引擎的页大小，单位为 KB，默认为 4 KB。范围为 1 到 16384，即 1 KB 到 16 MB。',
  precisionTip: '数据库的时间戳精度。ms 表示毫秒，us 表示微秒，ns 表示纳秒，默认 ms 毫秒。',
  replicaTip: '表示数据库副本数，取值为 1、2 或 3，默认为 {0}。在集群中使用，副本数必须小于或等于 DNODE 的数目。',
  retentionsTip:
    '表示数据的聚合周期和保存时长，如 RETENTIONS 15s:7d,1m:21d,15m:50d 表示数据原始采集周期为 15 秒，原始数据保存 7 天；按 1 分钟聚合的数据保存 21 天；按 15 分钟聚合的数据保存 50 天。目前支持且只支持三级存储周期。',
  strictTip: `表示数据同步的一致性要求，默认为 off。<ul><li>on 表示强一致，即运行标准的 raft 协议，半数提交返回成功。</li><li>off 表示弱一致，本地提交即返回成功。</li></ul>`,
  walLevelTip:
    'WAL 级别，默认为 1。\n    <ul>\n    <li>1：写 WAL，但不执行 fsync。</li>\n    <li>2：写 WAL，而且执行 fsync。</li>\n    </ul>',
  vgroupsTip: '数据库中初始 vgroup 的数目',
  singleStableTip:
    '表示此数据库中是否只可以创建一个超级表，用于超级表列非常多的情况。\n    <ul>\n    <li>0：表示可以创建多张超级表。</li>\n    <li>1：表示只可以创建一张超级表。</li>\n    </ul>',
  sttTaiggerTip:
    '表示落盘文件触发文件合并的个数。默认为 1，范围 1 到 16。对于少表高频场景，此参数建议使用默认配置，或较小的值；而对于多表低频场景，此参数建议配置较大的值。',
  tablePrefixTip:
    '当其为正值时，在决定把一个表分配到哪个 vgroup 时要忽略表名中指定长度的前缀；当其为负值时，在决定把一个表分配到哪个 vgroup 时只使用表名中指定长度的前缀；例如，假定表名为 "v30001"，当 TABLE_PREFIX = 2 时 使用 "0001" 来决定分配到哪个 vgroup ，当 TABLE_PREFIX = -2 时使用 "v3" 来决定分配到哪个 vgroup。',
  tableSuffixTip:
    '当其为正值时，在决定把一个表分配到哪个 vgroup 时要忽略表名中指定长度的后缀；当其为负值时，在决定把一个表分配到哪个 vgroup 时只使用表名中指定长度的后缀；例如，假定表名为 "v30001"，当 TABLE_SUFFIX = 2 时 使用 "v300" 来决定分配到哪个 vgroup ，当 TABLE_SUFFIX = -2 时使用 "01" 来决定分配到哪个 vgroup。',
  tsdbPagesizeTip:
    '一个 VNODE 中时序数据存储引擎的页大小，单位为 KB，默认为 4 KB。范围为 1 到 16384，即 1 KB到 16 MB。',
  walRetentionPeriodTip: `为了数据订阅消费，需要WAL日志文件额外保留的最大时长策略。WAL日志清理，不受订阅客户端消费状态影响。单位为 s。默认为 3600，表示在 WAL 保留最近 3600 秒的数据，请根据数据订阅的需要修改这个参数为适当值。\n如果您需要使用备份功能，此参数的设置必须大于备份周期+24小时`,
  walRetentionSizeTip:
    '为了数据订阅消费，需要WAL日志文件额外保留的最大累计大小策略。单位为 KB。默认为 0，表示累计大小无上限。',
  walRollPeriodTip:
    'wal 文件切换时长，单位为 s。当 wal 文件创建并写入后，经过该时间，会自动创建一个新的 wal 文件。单副本默认为 0，即仅在落盘时创建新文件。多副本默认为 1 天。',
  walSegmentSizeTip:
    'wal 单个文件大小，单位为 KB。当前写入文件大小超过上限后会自动创建一个新的 wal 文件。默认为 0，即仅在落盘时创建新文件。',
  replica1Tip: '目前是三节点高可用的实例，但是副本数是1的数据库是没有高可用的，请谨慎创建。',
  performanceRelatedParameters: '性能调优相关参数',
  dataPersistenceParameters: '数据持久化存储参数',
  s3KeepLocalTip:
    '数据在本地保留的天数，即 data 文件在本地磁盘保留多长时间后可以上传到 S3。缺省值为 365 天，取值范围 [1, 365000]，且必须大于或等于3倍的 duration 参数值。可以使用加单位的表示形式，如 S3_KEEPLOCAL 100h、S3_KEEPLOCAL 10d 等，支持 m（分钟）、h（小时）和 d（天）三个单位。',
  s3ChunkPagesTip:
    '上传对象的大小阈值，与 tsdb_pagesize 参数均不可修改，单位为 TSDB 页，只能配置为数字；缺省值为 262144 页，最小值为 131072 页，最大值 1048576 页。',
  s3CompactTip:
    '首次上传 S3 时，是否 compact 文件组。在触发 s3 数据上传流程时，会首先检查 data 文件是否进行过 compact 操作，如果没有并且 s3_compact 参数值为 1，则发起本文件组的 compact 操作，下次触发 s3 数据上传时会直接进行上传操作。',
  encryptTip:
    '指定数据采用的加密算法。默认是none，即不采用加密。sm4 表示采用 sm4 加密算法。目前只支持 sm4 加密算法，如果输入其它不支持的加密算法则会报错并建库失败',
  walParameters: 'WAL 配置参数',
  specialParameters: '特殊参数',
  delete: '删除数据库',
  viewDatabase: '查看数据库配置',
  managePrivilege: '管理数据库权限',
  operationLog: '操作日志',
  source: '源数据库',
  target: '目标数据库'
};
