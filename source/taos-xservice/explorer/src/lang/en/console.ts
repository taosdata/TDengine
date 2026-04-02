import { OEM_NAME } from '@/utils/init'

export default {
  console: {
    cellCopyTip: "Double-click the cell to copy the content",
    exec: "Exec",
    addFavorites: "Favorite",
    output: "output",
    history: "history",
    favorites: "Favorites",
    delFavirote: "Delete Favorite",
    addSharedFavirote: "Add Shared Favorite",
    persionalFavorites: "Personal Favorites",
    sharedFavorites: "Shared Favorites",
    inputEmpty: "Input is empty!",
    retrievedRow: "rows retrieved",
    grid: "Grid",
    chart: "Chart",
    chartType: "Chart Type",
    xAxis: "X Axis",
    series: "Series",
    draw: "Draw",
    log: "Log",
    export: "Export",
    info: "Info",
    CHILD_TABLEInfo: "Subtable Info",
    NORMAL_TABLEInfo: "Table Info",
    tableInfo: "Subtable Info",
    stableInfo: "STable Info",
    databaseInfo: "DB Info",
    content: "Content",
    sqlTip:
      "In SQL statement, table name must be prefixed with database name.\nFor example, select * from testdb.table1;",
    enterTip: "Please enter {value} value",
    sqlWarningTip:
      "The SQL you input doesn't have any conditions to limit the input and output, this may cause some problems such as too much time cost, too big result set, too big resource consumption. It's strongly recommended to add some conditions to filter by time range, tags, and use 'limit/slimit' to control the number of output groups.",
    startTime: "Please enter the start time.",
    endTime: "Please enter the end time.",
    category: "Category",
    name: "Name",
    type: "Type",
    stable_name: "STable Name",
    table_name: "Table Name",
    create_time: "Creation Time",
    columns: "Columns",
    tags: "Tags",
    encode: "Encoding",
    compress: "Compression Algorithm",
    level: "Compression Level",
    desc: "Description",
    share: "Shared favorites",
    unshare: "Unshare",
    addDesc: "Add description",
    editDesc: "Edit description",
    descPlaceholder: "Description of the SQL,no more than {0} characters.",
    characterLen: "Only {0} characters are allowed",
    addToPersonal: "Personal favorites"
  },
  sql: {
    analysis: "Analysis",
    slow: "Slow",
    periodTip: "Please select a time period",
    databaseTip: "Please select a database",
    typeTip: "Please select a type",
    approver: "Please input an approver",
    sqlTemplate: "Sql Template",
    cumulativeTime: "Cumulative Time",
    averageTime: "Average Time",
    execNum: "number of executions",
    endTime: "End Time",
    totalTime: "Total  Time",
    totalTimeTip: "Total Execution Time",
    last30: "Last 30 mins",
    endTimeTip: "End Run Time",
    slowTitle: "Slow Queries (latency longer than {time})",
  },
  data: {
    delRunningTaskBb:
      "Database '{dbName}' has been used by data source '{taskName}', please stop the task first",
    appendEditor: "Append Editor",
    viewData: "view 200 rows of data",
    changeDBPrivilegeTip:
      "Do you want to {type} the {listType} permissions from the {dbName} DB ?",
    exportDataViaCli: `Explorer data via ${OEM_NAME} CLI`,
    noDatabase: "No Database",
    dataExplorer: "Data Explorer",
    createStable: "Create STable",
    editStable: "Edit STable",
    createTable: "Create Table",
    editTable: "Edit Table",
    databases: "Databases",
    filterDatabase: "Filter Databases",
    addDatabase: "Add Database",
    dbName: "DB Name",
    createAt: "Created at",
    showTable: "Show Table",
    delStable: "Delete STable",
    databaseControl: "Database {0} Access Control",
    operations: "Operations",
    delTable: "Delete Table",
    createDatabase: "Create Database",
    viewDatabase: "View Database Config",
    editDatabase: "Edit Database",
    delDatabase: "Delete Database",
    viewStable: "View STable Config",
    manageDBprivilege: "Manage Database Privilege",
    viewTable: "View Table Config",
    createnormalTable: "Create Normal Table",
    createsubTable: "Create Sub Table",
    searchsp: "Search Super Table",
    searchnt: "Search Normal Table",
    searchsub: "Search Sub Table",
    searchtbtip: "Please enter the table name",
    fulltagtip: "Please enter full tag information",
    enabletag: "Enable TAG query",
    name: "Name",
    keep: "Keep",
    update: "Update",
    moreConfig: "More configuration parameters",
    configParams: "Configuration Parameters",
    Days: "Days",
    days: "days",
    precision: "Precision",
    bufferTip: "The size of cache for writing of a vnode, default is 256MB.",
    cacheModelTip: `specifies how the latest data in subtables is stored in the cache. The default value is none.
    <ul>
    <li>NONE - no caching.</li>
    <li>LAST_ROW - the last row of each table is cached and can significantly improve the performance of last_row() function.</li>
    <li>LAST_VALUE - the last non-NULL value of each column of each table is cached and can significantly improve the performance of last() without WHERE, ORDER BY, GROUP BY and INTERVAL.</li>
    <li>BOTH - equal to enabling LAST_VALUE and LAST_ROW together, the default value is NONE.</li>
    </ul>
    `,
    cacheSizeTip:
      "The size of cache for the latest data of each table in a vnode, the unit is MB, the default value is 1, maximum allowed value is 65536.",
    compTip: `The compression level of data file.
    <ul>
    <li>0 means no compression.</li>
    <li>1 means one phase compression.</li>
    <li>2 means two phase compression, the dfault value is 2.</li>
    </ul>
    `,
    durationTip:
      "The time range of data stored in a single file, its unit can be minute(m), hour(h), day(d), e.g. 10d, 100h， the default unit is day(d).",
    walFsyncPeriodTip:
      "The time interval of performing sync when WAL_LEVEL is set to 2, the unit is millisecond(ms), the default value is 3,000, i.e. 3000 milliseconds.",
    maxRowsTip:
      "The maximum number of rows stored in single data block, default value 4,096.",
    minRowsTip:
      "The minimum number of rows stored in single data block, default value is 100.",
    cacheLast: "CacheLast",
    nameTip: "The /name/ name is required!",
    daysTip:
      "DAYS is the time span for a data file to store data,<br />default: 10",
    precisionTip: `The precision at which a database records timestamps, ms for milliseconds, us for microseconds, or ns for nanoseconds, the default value is ms`,
    keepTip: `The number of days for keeping the data files, the default value is 3,650, the data files that have last over KEEP would be deleted automatically.You can use m (minutes), h (hours), and d (days) as the unit.`,
    updateTip: `data update level<br />
    0: data update is not supported<br />
    1: support update of entire row<br />
    2: support update only some columns<br />
    default: 0`,
    pagesTip:
      "The number of pages for caching meta data in a single vnode, the default value is 256 and the minimum allowed value is 64.",
    pageSizeTip:
      "The size of single page for caching meta data, the unit is KB, the value can be from 1 to 16,384, the default value is 4 KB.",
    replicaTip:
      "The number of replicas of the database, it can be set to 1 or 3, the default value is 1.",
    retentionsTip:
      "The time interval for aggregating and keeping data, for example 15s:7d,1m:21d,15m:50d indicates that data aggregated every 15 seconds is retained for 7 days, data aggregated every 1 minute is retained for 21 days, and data aggregated every 15 minutes is retained for 50 days. You must enter three aggregation intervals and corresponding retention periods.",
    strictTip: `表示数据同步的一致性要求，默认为 off。
    <ul>
    <li>on 表示强一致，即运行标准的 raft 协议，半数提交返回成功。</li>
    <li>off 表示弱一致，本地提交即返回成功。</li>
    </ul>
    `,
    walLevelTip: `specifies whether fsync is enabled. The default value is 1.
    <ul>
    <li>1 - Write data to WAL without fsync.</li>
    <li>2 - Write data to WAL with fsync.</li>
    </ul>
    `,
    vgroupsTip:
      "The number of vgroups of the database to be created, normally more vgroups means more processing capability but it is also limited by your system resources, the default value is 4.",
    singleStableTip: `whether the database can contain more than one supertable.
    <ul>
    <li>0 - The database can contain multiple supertables.</li>
    <li> 1 - The database can contain only one supertable, the default value is 0.</li>
    </ul>
    `,
    walRetentionPeriodTip:
      "The time length of keeping WAL files, it determines the data that can be consumed, the unit is second, the default value is 3600, a value of 0 which means no data can be consumed，please set to a proper positive value if you want to consume data.",
    walRetentionSizeTip:
      "The size of a single WAL file, the unit is KB, the default value is 0, which means it's handled automatically by TDengine.",
    walSegmentSizeTip:
      "The size of a single WAL file, the unit is KB, the default value is 0, which means it's handled automatically by TDengine.",
    walRollPeriodTip:
      "The time length of a single WAL file, the unit is second, the default value is 0, which means it's handled automatically by TDengine。",
    sttTaiggerTip:
      "The number of files that triggers data file merging, the default value is 1, the available range is from 1 to 16, lower value is suitable for fewer tables with high writing frequency but higher value is suitable for large number of tables with low writing frequency.",
    tsdbPagesizeTip:
      "The size of single page for caching time series data, the unit is KB, the value can be from 1 to 16,384, the default value is 4 KB.",
    tablePrefixTip: `The prefix in the table name that is ignored when distributing a table to a vgroup when it's a positive number, or only the prefix is used when distributing a table to a vgroup, the default value is 0; For example, if the table name v30001, then "0001" is used if TABLE_PREFIX is set to 2 but "v3" is used if TABLE_PREFIX is set to -2; It can help you to control the distribution of tables`,
    tableSuffixTip: `The suffix in the table name that is ignored when distributing a table to a vgroup when it's a positive number, or only the suffix is used when distributing a table to a vgroup, the default value is 0; For example, if the table name v30001, then "v300" is used if TABLE_SUFFIX is set to 2,  but "01" is used if TABLE_SUFFIX is set to -2; It can help you to control the distribution of tables`,
    stable: "STable",
    table: "Table",
    stableName: "STable Name",
    columns: "Columns",
    subTable: "Sub Table",
    columnNameTip: "Column Name",
    clickColumnTip: "Click Switch to TAG",
    tagNameTip: "Tag Name",
    sub_table: "SubTable",
    subTables: "SubTables",
    tableName: "Table Name",
    showData: "Show Data",
    createTableUse: "Create Sub Table",
    fields: "Fields",
    pageSize: "Page Size",
    selectAll: "Select All",
    checkFail: "Please check the field name or type",
    tableNameTip:
      "Table names are defaultly only allowed to consist of letters, numbers, and underscores, and cannot start with a number. The default is not case-sensitive. If you want to create a name that contains special characters or starts with a number or is case-sensitive, please add a ` before or after the name, for example, such as `testTable`.Special characters cannot be included \".\".",
    runSqlTip: "Run the first or selected SQL statement",
    performanceRelatedParameters: "Performance Related Parameters",
    dataPersistenceParameters: "Data Persistence Parameters",
    walParameters: "WAL Parameters",
    specialParameters: "Special Parameters",
    modifyColumn: "Modify column width",
    renameColumn: "Change column name",
    backslashTip: 'The database name is case-sensitive, for example, testDB and testdb are two different database names.',
  },
  explorer: {
    databases: "Databases",
    user_name: "User Name",
    privilege: "Privilege",
  },
}