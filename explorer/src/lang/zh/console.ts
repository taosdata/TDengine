import { OEM_NAME } from '@/utils/init'

export default {
  console: {
    cellCopyTip: "双击单元格进行复制",
    exec: "执行",
    addFavorites: "收藏",
    delFavirote: "删除收藏",
    output: "结果",
    history: "历史",
    favorites: "收藏",
    inputEmpty: "输入为空!",
    retrievedRow: "检索到的行",
    grid: "网格",
    chart: "图表",
    chartType: "图表类型",
    xAxis: "X轴",
    series: "系列",
    draw: "绘图",
    log: "日志",
    export: "导出",
    info: "信息",
    databaseInfo: "数据库信息",
    stableInfo: "超级表信息",
    CHILD_TABLEInfo: "子表信息",
    NORMAL_TABLEInfo: "表信息",
    tableInfo: "子表信息",
    persionalFavorites: "个人收藏",
    sharedFavorites: "共享收藏",
    sqlTip:
      "在SQL语句中，表名必须以数据库名作为前缀。\n例如，select * from testdb.table1;",
    enterTip: "请输入 {value} 值",
    sqlWaringTip:
      "当前查询无输入输出限制，可能导致查询时间长、资源消耗高、结果集过大等问题，如必要可添加主键时间范围、过滤条件、LIMIT/SLIMIT等限制条件进行优化",
    startTime: "请输入开始时间",
    endTime: "请输入结束时间",
    category: "类别",
    name: "名称",
    type: "类型",
    stable_name: "超级表名称",
    table_name: "表名",
    create_time: "创建时间",
    columns: "列数量",
    tags: "标签数量",
    encode: "编码方法",
    compress: "压缩算法",
    level: "压缩级别",
    desc: "描述",
    share: "添加到共享收藏",
    unshare: "取消共享",
    addDesc: "添加描述",
    editDesc: "编辑描述",
    descPlaceholder: "对收藏的 SQL 进行描述，可不填，不超过{0}个字符。",
    characterLen: "只允许输入{0}个字符",
    addToPersonal: "添加到个人收藏"
  },
  sql: {
    analysis: "分析",
    slow: "慢查询",
    periodTip: "请选择一个时间段",
    databaseTip: "请选择一个数据库",
    typeTip: "请选择类型",
    approver: "请输入审批人",
    sqlTemplate: "Sql模板",
    cumulativeTime: "累计耗时",
    averageTime: "平均耗时",
    execNum: "执行次数",
    endTime: "结束时间",
    endTimeTip: "结束运行时间",
    totalTime: "总时间",
    totalTimeTip: "总执行时间",
    last30: "最近30分钟",
    slowTitle: "慢查询（延迟超过 1 秒）",
  },
  data: {
    delRunningTaskBb:
      "数据库 '{dbName}' 已被数据源 '{taskName}' 使用，请先停止该任务",
    appendEditor: "追加到编辑器",
    viewData: "查看200行数据",
    changeDBPrivilegeTip: "你想从 {dbName} 数据库{type}{listType}权限吗?",
    exportDataViaCli: `通过${OEM_NAME} CLI交互`,
    noDatabase: "暂无数据库",
    dataExplorer: "数据浏览",
    createStable: "创建超级表",
    editStable: "编辑超级表",
    createTable: "创建表",
    editTable: "编辑表",
    databases: "数据库",
    filterDatabase: "筛选数据库",
    addDatabase: "新增数据库",
    editDatabase: "编辑数据库",
    delDatabase: "删除数据库",
    viewStable: "查看超级表配置",
    delStable: "删除超级表",
    delTable: "删除表",
    databaseControl: "数据库{0}访问控制",
    viewTable: "查看表配置",
    manageDBprivilege: "管理数据库权限",
    viewDatabase: "查看数据库配置",
    createnormalTable: "创建普通表",
    createsubTable: "创建子表",
    searchsp: "查询超级表",
    searchnt: "查询普通表",
    searchsub: "查询子表",
    searchtbtip: "请输入表名",
    fulltagtip: "请输入完整的TAG信息",
    enabletag: "是否开启TAG查询",
    dbName: "数据库名称",
    createAt: "创建时间",
    showTable: "展示表",
    operations: "操作",
    createDatabase: "创建数据库",
    name: "名称",
    keep: "保留",
    update: "更新",
    moreConfig: "更多配置参数",
    configParams: "配置参数",
    Days: "时间跨度",
    days: "天",
    precision: "时间精度",
    cacheLast: "缓存配置",
    nameTip: "/name/名称必填!",
    daysTip: "数据文件存储数据的时间跨度，</br> 默认为：10",
    precisionTip:
      "数据库的时间戳精度。ms 表示毫秒，us 表示微秒，ns 表示纳秒，默认 ms 毫秒",
    keepTip:
      "数据文件的保存天数，缺省值是 3650 天，超出 KEEP 所指定的保存天数的数据文件会被自动删除。支持 m（分钟）、h（小时）和 d（天）三个单位。也可以都不写单位。",
    cacheLastTip: `子表last_row是否缓存在内存中<br />
    范围：0-3<br />
    0：关闭；<br />
    1：缓存子表last_row；<br />
    2：缓存子表last_column无空值；<br />
    3：缓存子表last_row和last_column；<br />
    默认值：0`,
    updateTip: `数据更新级别<br />
    0：不支持数据更新<br />
    1：支持整行更新<br />
    2：只支持更新部分列<br />
    默认值：0`,
    cacheModelTip: `是否缓存每个子表最近的数据。默认为 NONE。
    <ul>
    <li>NONE - 表示不缓存。</li>
    <li>LAST_ROW - 缓存每个子表的最后一条记录。</li>
    <li>LAST_VALUE - 缓存每个子表每个列的最后一个非 NULL 值。</li>
    <li>BOTH - 等同于同时打开 LAST_VALUE 和 LAST_ROW， 缺省值是 NONE。</li>
    </ul>
    `,
    cacheSizeTip:
      "vnode 中缓存每张表最新数据的缓存大小，单位是 MB，缺省值是 1， 最大允许值是 65536",
    compTip: `数据文件的压缩级别。
    <ul>
    <li>0 - 不压缩。</li>
    <li>1 - 一阶段压缩。</li>
    <li>2 - 两阶段压缩， 缺省值是 2。</li>
    </ul>
    `,
    vgroupsTip:
      "vgroup 的数量，一般来说更多的 vgroup 意味着更多的处理能力，前提是系统中有足够的资源，默认值是2。",
    singleStableTip: `数据库中是否只允许创建单个超级表。
    <ul>
    <li> 0 - 可以创建多个超级表。</li>
    <li> 1 - 只能创建一个超级表，缺省值为0。</li>
    </ul>
    `,
    walLevelTip: `WAL 级别，默认为 1。
    <ul>
    <li>1 - 数据写入 WAL 但不执行 fsync。</li>
    <li>2 - 数据写入 WAL 且执行 fsync。</li>
    </ul>
    `,
    walRetentionPeriodTip:
      "WAL 文件的最大保存时长，它决定了能够订阅到的数据，单位是秒，默认值是3600，值为0时意味着没有数据可以消费，如果想订阅数据请设置为合适的正值。",
    walRetentionSizeTip:
      "为了数据订阅消费，需要WAL日志文件额外保留的最大累计大小策略。单位为 KB。默认为 0，表示累计大小无上限。",
    pagesTip:
      "单个 vnode 中缓存元数据的缓存页数，缺省值是 256，该值允许配置的最小值是 64",
    pageSizeTip:
      "vnode 中元数据缓存的页大小，单位是 KB ，值域是 [1,16384]，缺省值是 4 KB。",
    replicaTip: "数据库副本数，取值为 1 或 3，默认为 1。",
    retentionsTip:
      "数据的聚合周期和保存时长，如 RETENTIONS 15s:7d,1m:21d,15m:50d 表示数据原始采集周期为 15 秒，原始数据保存 7 天；按 1 分钟聚合的数据保存 21 天；按 15 分钟聚合的数据保存 50 天。目前支持且只支持三级存储周期。",
    strictTip: `表示数据同步的一致性要求，默认为 off。
    <ul>
    <li>on 表示强一致，即运行标准的 raft 协议，半数提交返回成功。</li>
    <li>off 表示弱一致，本地提交即返回成功。</li>
    </ul>
    `,
    walFsyncPeriodTip:
      " 当 WAL_LEVEL 设置为 2 时执行 fynsc 的周期，单位是毫秒，默认值是3000，即3000毫秒。",
    maxRowsTip: "单个数据块中存储的最大记录数量，缺省值为 4096。",
    minRowsTip: "单个数据块中存储的最小记录数量，缺省值为 100 。",
    bufferTip:
      "每个 vnode 的写入缓存大小，单位为 MB，默认为 256，最小为 3，最大为 16384",
    durationTip:
      "每个数据存储所存储的数据的时间跨度，其单位可以是分钟(m)，小时(h)，天(d)，默认单位是天，如 10d, 1000h。",
    walRollPeriodTip:
      "单个 WAL 文件中保存的数据时长，单位是秒，默认值为0，意味着 TDengine 会自动处理。",
    walSegmentSizeTip:
      "单个 WAL 文件的大小上限，单位是 KB，默认值为0，意味着 TDengine 会自动处理。",
    sttTaiggerTip:
      "触发落盘文件合并的文件数量，缺省值是1，可选值是1到16，表数少写入频率越高适用于较小的值，表数多写入频率低适用较大的值。",
    tsdbPagesizeTip:
      "vnode 中缓存时序数据的页大小，单位是 KB ，值域是 [1,16384]，缺省值是 4 KB。",
    tablePrefixTip: `当其为正值时，在决定把一个表分配到哪个 vgroup 时要忽略表名中指定长度的前缀；当其为负值时，在决定把一个表分配到哪个 vgroup 时只使用表名中指定长度的前缀；例如，假定表名为 "v30001"，当 TABLE_PREFIX = 2 时 使用 "0001" 来决定分配到哪个 vgroup ，当 TABLE_PREFIX = -2 时使用 "v3" 来决定分配到哪个 vgroup。`,
    tableSuffixTip: `当其为正值时，在决定把一个表分配到哪个 vgroup 时要忽略表名中指定长度的后缀；当其为负值时，在决定把一个表分配到哪个 vgroup 时只使用表名中指定长度的后缀；例如，假定表名为 "v30001"，当 TABLE_SUFFIX = 2 时 使用 "v300" 来决定分配到哪个 vgroup ，当 TABLE_SUFFIX = -2 时使用 "01" 来决定分配到哪个 vgroup。`,
    stable: "超级表",
    table: "数据表",
    stableName: "超级表名称",
    columns: "列",
    columnNameTip: "列名",
    clickColumnTip: "点击切换为标签",
    tagNameTip: "标签名",
    subTable: "子表",
    sub_table: "子表",
    subTables: "子表",
    tableName: "表名",
    showData: "展示数据",
    createTableUse: "创建超级表",
    fields: "字段",
    pageSize: "每页条数",
    selectAll: "全选",
    checkFail: "请检查字段名或类型",
    tableNameTip:
      "表名默认只能由字母、数字和下划线组成，且不能以数字开头。默认不区分大小写，如果要创建包含特殊字符或以数字开头或区分大小写的名称，请在名称前后添加 `，例如`testTable`。且特殊字符不能包含“.“。",
    runSqlTip: "运行第一条或选中的 SQL 语句",
    performanceRelatedParameters: "性能调优相关参数",
    dataPersistenceParameters: "数据持久化存储参数",
    walParameters: "WAL 配置参数",
    specialParameters: "特殊参数",
    modifyColumn: "修改列宽",
    renameColumn: "修改列名",
    backslashTip: '如果要创建区分大小写的数据库名称，请在名称前后添加 `。例如，`testDB`。',
  },
  explorer: {
    databases: "数据库",
    user_name: "用户名",
    privilege: "权限",
  },
}