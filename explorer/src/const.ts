import mitt from 'mitt';
import i18n from '@/lang/index.ts';
import { computed } from 'vue';
export const BaseRoute = [
  '/instances',
  '/billing',
  '/alert',
  '/activity',
  '/profile',
  '/support',
  '/instanceStatus',
  '/user',
  '/landing',
  '/calculator'
];
export const isEn = computed(() => (i18n.global.locale as WritableComputedRef<string>).value == 'en');
export const OfficialSite = computed(() => (isEn.value ? 'https://tdengine.com' : 'https://taosdata.com'));

export const TDengineFnReverseGroup = ['CONCAT_WS', 'CONCAT'];

export const NeedRefreshStatus = [
  'Suspending',
  'Starting',
  'Ready',
  'Stopping',
  'Expanding', //扩容中
  'Shrinking', //缩容中
  'Upgrading' //升级中
];
export const PermissionMap = {
  1: 1,
  2: 0
};
export const RedirectKey = 'TDengine-Redirect';
export const ReLoginCode = ['502', '401', '432']; //重新登录的状态码
export const SuccessCode = ['200', '302']; //请求成功的状态码
export const ServerLevel = {
  FREE: 0,
  STANDARD: 1,
  ENTERPRISE: 2,
  0: 'FREE',
  1: 'STANDARD',
  2: 'ENTERPRISE'
};
export const OFFSETUTCTIME = new Date().getTimezoneOffset() * 60 * 1000;

export const VariableTableColumnType = ['BINARY', 'NCHAR', 'VARCHAR', 'VARBINARY', 'GEOMETRY'];
export const HIDEDB = ['information_schema', 'performance_schema'];
export const DBFILED: Recordable<Recordable> = {
  buffer: { type: 'number', alter: false, defaultValue: 32 },
  cachemodel: { type: 'string', alter: true, defaultValue: 'none' },
  cachesize: { type: 'number', alter: true, defaultValue: 1 },
  comp: { type: 'number', alter: false, defaultValue: 2 },
  duration: { type: 'number', alter: false, defaultValue: '10d' },
  wal_fsync_period: { type: 'number', alter: true, defaultValue: 3000 },
  maxrows: { type: 'number', alter: false, defaultValue: 4096 },
  minrows: { type: 'number', alter: false, defaultValue: 100 },
  keep: { type: 'number', alter: true, defaultValue: 3650 },
  pages: { type: 'number', alter: false, defaultValue: 256 },
  pagesize: { type: 'number', alter: false, defaultValue: 4 },
  precision: { type: 'string', alter: false, defaultValue: 'ms' },
  replica: { type: 'number', alter: false, defaultValue: 1 },
  retentions: { type: 'string', alter: false, defaultValue: '' },
  strict: { type: 'string', alter: false, defaultValue: 'off', version: '<=3.0.2.4' },
  wal_level: { type: 'number', alter: true, defaultValue: 1 },
  vgroups: { type: 'number', alter: false, defaultValue: 4 },
  single_stable: { type: 'number', alter: false, defaultValue: 0 },
  wal_retention_period: { type: 'number', alter: false, defaultValue: 3600 }, //
  wal_retention_size: { type: 'number', alter: false, defaultValue: 0 },
  wal_roll_period: { type: 'number', alter: false, defaultValue: 0, version: '<=3.0.7.1' },
  wal_segment_size: { type: 'number', alter: false, defaultValue: 0, version: '<=3.0.7.1' },
  stt_trigger: { type: 'number', alter: false, defaultValue: 1, version: '>=3.0.5.0' },
  tsdb_pagesize: { type: 'number', alter: false, defaultValue: 4, version: '>=3.0.5.0' },
  table_prefix: { type: 'number', alter: false, defaultValue: undefined, version: '>=3.0.5.0' },
  table_suffix: { type: 'number', alter: false, defaultValue: undefined, version: '>=3.0.5.0' },
  s3_keeplocal: { type: 'number', alter: true, defaultValue: '365d', version: '>=3.3.4.3' },
  s3_chunkpages: { type: 'number', defaultValue: 262144, alter: false, version: '>=3.3.4.3' },
  s3_compact: { type: 'number', alter: true, defaultValue: '1', version: '>=3.3.4.3' },
  ENCRYPT_ALGORITHM: { type: 'string', alter: false, defaultValue: 'none', version: '>=3.3.0.0' }
};
export const DBCustomedFiled = [
  'parent',
  'node-key',
  'typeName',
  'privileges',
  'databaseId',
  'databaseName',
  'databaseAccessType'
];
export const TokenExpire = 1 / 24; //day
export const AppIDKey = 'AppID';
export const TokenKey = 'TDengine-Token';
export const OAuthTokenKey = 'oauth_token';
export const SessionIdKey = 'session_id';
export const BaseUrlKey = 'base_url';

// export const StreamDocsUrl = i18n.global.locale.value?.includes('en') ? "https://docs.tdengine.com/develop/stream/" : 'https://docs.taosdata.com/develop/stream/';
export const DocsUrl = 'https://docs.tdengine.com';

export const TdengineVersion = '3.2.0.0';
// export const SubscriptionDocsUrl = i18n.global.locale.value?.includes('en') ? "https://docs.tdengine.com/taos-sql/tmq/#create-a-topic" : 'https://docs.taosdata.com/taos-sql/tmq/#create-a-topic';
export const TDengineTimeUnit = [
  {
    label: 'nanosecond',
    value: 'b'
  },
  {
    label: 'microsecond',
    value: 'u'
  },
  {
    label: 'millisecond',
    value: 'a'
  },
  {
    label: 'second',
    value: 's'
  },
  {
    label: 'minute',
    value: 'm'
  },
  {
    label: 'hour',
    value: 'h'
  },
  {
    label: 'day',
    value: 'd'
  },
  {
    label: 'week',
    value: 'w'
  },
  {
    label: 'month',
    value: 'n'
  },
  {
    label: 'year',
    value: 'y'
  }
];

export const SlowSqlTime = '200ms';

export const ReplicationTaskStatus = [
  'created',
  'failed',
  'cancelled',
  'deleted',
  'completed',
  'interrupted',
  'stopped',
  'running'
];
export const ReplicationTaskCanStopStatus = ['interrupted', 'running', 'created'];

export const ReplicationTaskCanStartStatus = ['stopped'];

export const $bus = mitt();

export const CustomShellContent = ['Welcome to TDengine '];

export const IntegerType = [
  'int',
  'int unsigned',
  'bigint',
  'bigint unsigned',
  'float',
  'double',
  'smallint',
  'smallint unsigned',
  'tinyint',
  'tinyint unsigned'
];
export const StringType = ['varchar', 'nchar', 'binary'];
// 数学函数
export const NumericFn = [
  {
    label: 'ABS',
    supportDatatype: IntegerType
  },
  {
    label: 'ACOS',
    supportDatatype: IntegerType
  },
  {
    label: 'ASIN',
    supportDatatype: IntegerType
  },
  {
    label: 'ATAN',
    supportDatatype: IntegerType
  },
  {
    label: 'CEIL',
    supportDatatype: IntegerType
  },
  {
    label: 'COS',
    supportDatatype: IntegerType
  },
  {
    label: 'FLOOR',
    supportDatatype: IntegerType
  },
  {
    label: 'LOG',
    supportDatatype: IntegerType,
    filters: [
      {
        type: 'select',
        label: 'Log Filed',
        options() {
          return this.props.fieldList.filter(item => item.field != this.field);
        },
        placeholder: 'Select Filed',
        field: 'logFiled',
        defaultValue: ''
      }
    ]
  },
  {
    label: 'POW',
    supportDatatype: IntegerType,
    filters: [
      {
        type: 'select',
        label: 'Pow Filed',
        options() {
          return this.props.fieldList.filter(item => item.field != this.field);
        },
        placeholder: 'Select Filed',
        field: 'powFiled',
        defaultValue: ''
      }
    ]
  },
  {
    label: 'ROUND',
    supportDatatype: IntegerType
  },
  {
    label: 'SIN',
    supportDatatype: IntegerType
  },
  {
    label: 'SQRT',
    supportDatatype: IntegerType
  },
  {
    label: 'TAN',
    supportDatatype: IntegerType
  }
];
// 字符串函数
export const StringFn = [
  {
    label: 'CHAR_LENGTH',
    supportDatatype: StringType
  },
  {
    label: 'CONCAT',
    supportDatatype: StringType,
    filters: [
      {
        type: 'select',
        label: 'Concat Fields',
        multiple: true,
        options() {
          return this.props.fieldList.filter(item => item.field != this.field);
        },
        placeholder: 'Select Fields',
        field: 'concatFields',
        defaultValue: []
      }
    ]
  },
  {
    label: 'CONCAT_WS',
    supportDatatype: StringType,
    filters: [
      {
        type: 'input',
        label: 'Separator Fields',
        placeholder: 'Separator Fields',
        field: 'separatorFields',
        defaultValue: ''
      },
      {
        type: 'select',
        label: 'Concat Fields',
        multiple: true,
        options() {
          return this.props.fieldList.filter(item => item.field != this.field);
        },
        placeholder: 'Select Fields',
        field: 'concatFields',
        defaultValue: []
      }
    ]
  },
  {
    label: 'LENGTH',
    supportDatatype: StringType
  },
  {
    label: 'LOWER',
    supportDatatype: StringType
  },
  {
    label: 'LTRIM',
    supportDatatype: StringType
  },
  {
    label: 'RTRIM',
    supportDatatype: StringType
  },
  {
    label: 'SUBSTR',
    supportDatatype: StringType,
    filters: [
      {
        type: 'number',
        label: 'Position Fields',
        placeholder: 'Position Fields',
        field: 'posFields',
        defaultValue: ''
      },
      {
        type: 'number',
        label: 'length',
        placeholder: 'length',
        field: 'len',
        defaultValue: ''
      }
    ]
  },
  {
    label: 'UPPER',
    supportDatatype: StringType
  }
];

// 转换函数
export const ConversionFn = [
  {
    label: 'CAST',
    supportDatatype: StringType.concat(IntegerType)
  },
  {
    label: 'TO_ISO8601',
    supportDatatype: ['int', 'timestamp']
  },
  {
    label: 'TO_JSON',
    supportDatatype: ['json']
  },
  {
    label: 'TO_UNIXTIMESTAMP',
    supportDatatype: ['varchar', 'nchar']
  }
];
// 时间和日期函数
export const DatetimeFN = [
  {
    label: 'NOW',
    supportDatatype: ['timestamp']
  },
  {
    label: 'TIMEDIFF',
    supportDatatype: ['timestamp'],
    filters: [
      {
        type: 'string',
        label: 'TimeUnit Fields',
        placeholder: 'TimeUnit Fields',
        field: 'timeunitFields',
        options: [
          {
            label: 'nanosecond',
            value: '1b'
          },
          {
            label: 'microsecond',
            value: '1u'
          },
          {
            label: 'millisecond',
            value: '1a'
          },
          {
            label: 'second',
            value: '1s'
          },
          {
            label: 'minute',
            value: '1m'
          },
          {
            label: 'hour',
            value: '1h'
          },
          {
            label: 'day',
            value: '1d'
          },
          {
            label: 'week',
            value: '1w'
          }
        ]
      }
    ]
  },
  {
    label: 'TIMETRUNCATE',
    supportDatatype: ['timestamp'],
    filters: [
      {
        type: 'string',
        label: 'TimeUnit Fields',
        placeholder: 'TimeUnit Fields',
        field: 'timeunitFields',
        options: [
          {
            label: 'nanosecond',
            value: '1b'
          },
          {
            label: 'microsecond',
            value: '1u'
          },
          {
            label: 'millisecond',
            value: '1a'
          },
          {
            label: 'second',
            value: '1s'
          },
          {
            label: 'minute',
            value: '1m'
          },
          {
            label: 'hour',
            value: '1h'
          },
          {
            label: 'day',
            value: '1d'
          },
          {
            label: 'week',
            value: '1w'
          }
        ]
      },
      {
        type: 'number',
        label: 'ignore_timezone',
        placeholder: 'ignore_timezone',
        defaultValue: 1,
        field: 'ignore_timezone'
      }
    ]
  },
  {
    label: 'TIMEZONE',
    supportDatatype: ['timestamp']
  },
  {
    label: 'TODAY',
    supportDatatype: ['timestamp']
  }
];
// 聚合函数
export const AggregationFn = [
  {
    label: 'APERCENTILE',
    supportTopic: false,
    supportDatatype: IntegerType,
    filters: [
      {
        type: 'number',
        label: 'p',
        placeholder: 'p',
        min: 0,
        max: 100,
        field: 'p',
        defaultValue: 0
      },
      {
        type: 'select',
        label: 'algo_type',
        options: [
          {
            label: 'default',
            value: 'default'
          },
          {
            label: 't-digest',
            value: 't-digest'
          }
        ],
        placeholder: 'algo_type',
        field: 'algo_type',
        defaultValue: 'default'
      }
    ]
  },
  {
    label: 'AVG',
    supportTopic: false,
    supportDatatype: IntegerType
  },
  {
    label: 'COUNT',
    supportTopic: false,
    supportDatatype: ['all']
  },
  {
    label: 'ELAPSED',
    supportTopic: false,
    supportStream: false,
    supportDatatype: ['timestamp'],
    include: ['TIMESTAMP'],
    filters: [
      {
        type: 'select',
        label: 'TimeUnit Fields',
        options: [
          {
            label: 'nanosecond',
            value: '1b'
          },
          {
            label: 'microsecond',
            value: '1u'
          },
          {
            label: 'millisecond',
            value: '1a'
          },
          {
            label: 'second',
            value: '1s'
          },
          {
            label: 'minute',
            value: '1m'
          },
          {
            label: 'hour',
            value: '1h'
          },
          {
            label: 'day',
            value: '1d'
          },
          {
            label: 'week',
            value: '1w'
          }
        ],
        placeholder: 'TimeUnit Fields',
        field: 'timeunitFields',
        defaultValue: ''
      }
    ]
  },
  {
    label: 'LEASTSQUARES',
    supportStream: false,
    supportTopic: false,
    supportDatatype: IntegerType,
    filters: [
      {
        type: 'number',
        label: 'Start Fields',
        placeholder: 'Start Fields',
        min: 0,
        field: 'startFields',
        defaultValue: 0
      },
      {
        type: 'number',
        label: 'Step Fields',
        placeholder: 'Step Fields',
        min: 0,
        field: 'stepFields',
        defaultValue: 0
      }
    ]
  },
  // {
  //   label: "MODE",
  //   supportTopic:false,
  //   supportStream: false,
  //   supportDatatype:['all']
  // },
  {
    label: 'SPREAD',
    supportTopic: false,
    supportDatatype: ['int', 'timestamp']
  },
  {
    label: 'STDDEV',
    supportTopic: false,
    supportDatatype: IntegerType
  },
  {
    label: 'SUM',
    supportTopic: false,
    supportDatatype: IntegerType
  },
  {
    label: 'HYPERLOGLOG',
    supportTopic: false,
    supportDatatype: ['all']
  },
  // {
  //   label: "HIPERLOGLOG",
  //   supportTopic:false,
  //   supportDatatype:['all']
  // },
  {
    label: 'HISTOGRAM',
    supportTopic: false,
    supportStream: false,
    supportDatatype: IntegerType,
    filters: [
      {
        type: 'select',
        label: 'BinType Fields',
        options: [
          {
            label: 'user_input',
            value: 'user_input'
          },
          {
            label: 'linear_bin',
            value: 'linear_bin'
          },
          {
            label: 'log_bin',
            value: 'log_bin'
          }
        ],
        placeholder: 'BinType Fields',
        field: 'bintypeFields',
        defaultValue: ''
      },
      {
        type: 'input',
        label: 'Description Fields',
        placeholder: 'Description Fields',
        field: 'descriptionFields',
        defaultValue: ''
      }
    ]
  },
  {
    label: 'PERCENTILE',
    supportTopic: false,
    supportStream: false,
    supportDatatype: IntegerType,
    filters: [
      {
        type: 'number',
        label: 'p',
        placeholder: 'p',
        min: 0,
        max: 100,
        field: 'p',
        defaultValue: 0
      }
    ]
  }
];
// 选择函数
export const SelectorFn = [
  {
    label: 'BOTTOM',
    supportStream: false,
    supportTopic: false,
    supportDatatype: IntegerType,
    filters: [
      {
        type: 'number',
        label: 'k',
        placeholder: 'k',
        min: 1,
        max: 100,
        field: 'k',
        defaultValue: 1
      }
    ]
  },
  {
    label: 'FIRST',
    supportTopic: false,
    supportDatatype: ['all']
  },
  {
    label: 'INTERP',
    supportTopic: false,
    supportStream: false,
    supportDatatype: IntegerType
  },
  {
    label: 'LAST',
    supportTopic: false,
    supportDatatype: ['all']
  },
  {
    label: 'LAST_ROW',
    supportTopic: false,
    supportDatatype: ['all']
  },
  {
    label: 'MAX',
    supportTopic: false,
    supportDatatype: IntegerType
  },
  {
    label: 'MIN',
    supportTopic: false,
    supportDatatype: IntegerType
  },
  {
    label: 'MODE',
    supportStream: false,
    supportTopic: false,
    supportDatatype: ['all']
  },
  {
    label: 'SAMPLE',
    supportTopic: false,
    supportStream: false,
    supportDatatype: ['all'],
    filters: [
      {
        type: 'number',
        label: 'k',
        placeholder: 'k',
        min: 1,
        max: 100,
        field: 'k',
        defaultValue: 1
      }
    ]
  },
  {
    label: 'TAIL',
    supportStream: false,
    supportTopic: false,
    supportDatatype: IntegerType.concat(StringType),
    filters: [
      {
        type: 'number',
        label: 'k',
        placeholder: 'k',
        min: 1,
        max: 100,
        field: 'k',
        defaultValue: 1
      },
      {
        type: 'number',
        label: 'offset_rows',
        placeholder: 'offset_rows',
        min: 0,
        max: 100,
        field: 'offset_rows',
        defaultValue: 0
      }
    ]
  },
  {
    label: 'TOP',
    supportStream: false,
    supportTopic: false,
    supportDatatype: IntegerType,
    filters: [
      {
        type: 'number',
        label: 'k',
        placeholder: 'k',
        min: 1,
        max: 100,
        field: 'k',
        defaultValue: 1
      }
    ]
  },
  {
    label: 'UNIQUE',
    supportTopic: false,
    supportStream: false,
    supportDatatype: IntegerType.concat(StringType)
  }
];
// 时序数据特有函数
export const SeriesSpecificFn = [
  {
    label: 'CSUM',
    supportStream: false,
    supportDatatype: IntegerType
  },
  {
    label: 'DERIVATIVE',
    supportStream: false,
    supportDatatype: IntegerType,
    filters: [
      {
        type: 'number',
        label: 'Interval Fields',
        placeholder: 'Interval Fields',
        min: 1,
        field: 'intervalFields',
        defaultValue: 1
      },
      {
        type: 'select',
        label: 'Ignore Negative Fields',
        placeholder: 'Ignore Negative Fields',
        field: 'ignorenegative',
        defaultValue: 1,
        options: [
          {
            label: 'normal', ////////需要指定名称？？？
            value: 0
          },
          {
            label: 'negative',
            value: 1
          }
        ]
      }
    ]
  },
  {
    label: 'DIFF',
    supportStream: false,
    supportDatatype: IntegerType,
    filters: [
      {
        type: 'select',
        label: 'Negative Fields',
        placeholder: 'Negative Fields',
        field: 'negativeFields',
        defaultValue: 0,
        options: [
          {
            label: 'normal', ////////需要指定名称？？？
            value: 0
          },
          {
            label: 'negative',
            value: 1
          }
        ]
      }
    ]
  },
  {
    label: 'IRATE',
    supportStream: false,
    supportDatatype: IntegerType
  },
  {
    label: 'MAVG',
    supportStream: false,
    supportDatatype: IntegerType,
    filters: [
      {
        type: 'number',
        label: 'k',
        placeholder: 'k',
        min: 1,
        max: 1000,
        field: 'k',
        defaultValue: 1
      }
    ]
  },
  {
    label: 'STATECOUNT',
    supportStream: false,
    supportDatatype: IntegerType,
    filters: [
      {
        type: 'number',
        label: 'Value Fields',
        placeholder: 'Value Fields',
        field: 'valueFields',
        defaultValue: 1
      },
      {
        type: 'select',
        label: 'Operation Fields',
        placeholder: 'Operation Fields',
        field: 'operationFields',
        options: [
          {
            label: 'LT',
            value: 'LT'
          },
          {
            label: 'GT',
            value: 'GT'
          },
          {
            label: 'LE',
            value: 'LE'
          },
          {
            label: 'GE',
            value: 'GE'
          },
          {
            label: 'NE',
            value: 'NE'
          },
          {
            label: 'EQ',
            value: 'EQ'
          }
        ]
      }
    ]
  },
  {
    label: 'STATEDURATION',
    supportStream: false,
    supportDatatype: IntegerType,
    filters: [
      {
        type: 'number',
        label: 'Value Fields',
        placeholder: 'Value Fields',
        field: 'valueFields',
        defaultValue: 1
      },
      {
        type: 'select',
        label: 'Operation Fields',
        placeholder: 'oper',
        field: 'oper',
        options: [
          {
            label: 'LT',
            value: "'LT'"
          },
          {
            label: 'GT',
            value: "'GT'"
          },
          {
            label: 'LE',
            value: "'LE'"
          },
          {
            label: 'GE',
            value: "'GE'"
          },
          {
            label: 'NE',
            value: "'NE'"
          },
          {
            label: 'EQ',
            value: "'EQ'"
          }
        ]
      },
      {
        type: 'select',
        label: 'unit',
        options: [
          {
            label: 'nanosecond',
            value: '1b'
          },
          {
            label: 'microsecond',
            value: '1u'
          },
          {
            label: 'millisecond',
            value: '1a'
          },
          {
            label: 'second',
            value: '1s'
          },
          {
            label: 'minute',
            value: '1m'
          },
          {
            label: 'hour',
            value: '1h'
          },
          {
            label: 'day',
            value: '1d'
          },
          {
            label: 'week',
            value: '1w'
          }
        ],
        placeholder: 'unit',
        field: 'unit',
        defaultValue: ''
      }
    ]
  },
  {
    label: 'TWA',
    supportStream: false,
    supportDatatype: IntegerType
  }
];
// 系统信息函数
export const SystemFn = [
  {
    label: 'DATABASE',
    supportDatatype: ['system']
  },
  {
    label: 'CLIENT_VERSION',
    supportDatatype: ['system']
  },
  {
    label: 'SERVER_VERSION',
    supportDatatype: ['system']
  },
  {
    label: 'SERVER_STATUS',
    supportDatatype: ['system']
  }
];

export const TDengineStringType = ['VARCHAR', 'BINARY', 'NCHAR'];
export const TDengineNumberType = [
  'INT',
  'INT UNSIGNED',
  'BIGINT',
  'BIGINT UNSIGNED',
  'FLOAT',
  'DOUBLE',
  'SMALLINT',
  'SMALLINT UNSIGNED',
  'TINYINT',
  'TINYINT UNSIGNED'
];
// 时间戳可使用的运算符
export const CompareOperator = ['>', '<', '>=', '<=', '!=', '='];
export const BooleanOperator = ['='];
export const JsonOperator = ['CONTAINS', 'IS NULL', 'IS NOT NULL'];
export const GeneralOperator = [
  {
    label: 'IN',
    exclude: ['JSON']
  },
  {
    label: 'BETWEEN'
  },
  {
    label: 'NOT IN'
  },
  {
    label: 'NOT BETWEEN'
  },
  {
    label: 'IS NULL'
  },
  {
    label: 'IS NOT NULL'
  }
];
export const RegularOperator = ['MATCH', 'NMATCH', 'LIKE', 'NOT LIKE'];

export const TDengineFill = ['NONE', 'VALUE', 'PREV', 'NULL', 'LINEAR', 'NEXT', 'NULL_F', 'VALUE_F'];

export const backupMockData = [
  {
    to_expand: { path: '/data/test' },
    database: 'myDatabase',
    created_at: '2024-03-28T13:38:06+08:00',
    status: 'stopped'
  }
];

export const replicationMockData = [
  {
    id: '1',
    fromdb: 'myDatabase',
    hostport: 'taos+ws://root:taosdata@192.168.1.10:6041/mytest',
    status: 'stopped',
    reason: 'Task has been stopped',
    finished_at: '2024-03-28T13:38:06+08:00',
    created_at: '2024-03-28T13:38:06+08:00'
  }
];

export const licenseMockData = {};

export const auditMockData = [
  {
    ts: '2024-03-28T13:37:06+08:00',
    client_address: '127.0.0.1:60640',
    user_name: 'root',
    operation: 'createStb',
    db: 'myDatabase',
    resource: 'meteralltype',
    details: 'dbname:myDatabase, stable name:meteralltype'
  },
  {
    ts: '2024-03-28T13:36:06+08:00',
    client_address: '127.0.0.1:60640',
    user_name: 'root',
    operation: 'login',
    db: '',
    resource: '',
    details: 'app:taosadapter'
  }
];

export const dataInMockData = [
  {
    taskid: 1,
    id: 1,
    name: 'td3-demo',
    localname: 'td3',
    localtype: 'TDengine Data Subscription',
    target: 'targetDatabase',
    created_at: '2024-03-27T10:34:15.994Z',
    finished_at: '2024-03-27T21:20:51.681Z',
    status: 'completed',
    completed: true,
    taskActivities: [
      {
        level: 'info',
        at: '2024-03-27T21:20:51.681Z',
        activity: '',
        context: ''
      }
    ]
  }
];

export const agentMockData = [
  {
    id: 1,
    name: 'test',
    status: 'created',
    created_at: '2024-03-27T21:20:51.681Z',
    agentActivities: [
      {
        level: 'info',
        at: '2024-03-27T21:20:51.681Z',
        activity: '',
        context: ''
      }
    ]
  }
];
