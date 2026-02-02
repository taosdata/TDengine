import { t } from 'locales';
// 数学函数
export const NumericFn: TDFnType[] = [
  {
    label: 'ABS'
  },
  {
    label: 'ACOS'
  },
  {
    label: 'ASIN'
  },
  {
    label: 'ATAN'
  },
  {
    label: 'CEIL'
  },
  {
    label: 'COS'
  },
  {
    label: 'FLOOR'
  },
  {
    label: 'LOG',
    filters: [
      {
        type: 'select',
        label: 'Log Field',
        options(_, fieldList) {
          return fieldList;
        },
        placeholder: 'Select Field',
        field: 'logField',
        defaultValue: ''
      }
    ]
  },
  {
    label: 'POW',
    filters: [
      {
        type: 'select',
        label: 'Pow Field',
        options(_, fieldList) {
          return fieldList;
        },
        placeholder: 'Select Field',
        field: 'powField',
        defaultValue: ''
      }
    ]
  },
  {
    label: 'ROUND'
  },
  {
    label: 'SIN'
  },
  {
    label: 'SQRT'
  },
  {
    label: 'TAN'
  }
];
// 字符串函数
export const StringFn: TDFnType[] = [
  {
    label: 'CHAR_LENGTH'
  },
  {
    label: 'CONCAT',
    filters: [
      {
        type: 'select',
        label: 'Concat Fields',
        multiple: true,
        collapseTags: true,
        options(_, fieldList) {
          return fieldList;
        },
        placeholder: 'Select Fields',
        field: 'concatFields',
        defaultValue: []
      }
    ]
  },
  {
    label: 'CONCAT_WS',
    filters: [
      {
        type: 'input',
        label: 'Separator',
        placeholder: 'Separator_expr',
        field: 'separator',
        defaultValue: ''
      },
      {
        type: 'select',
        label: 'Concat Fields',
        multiple: true,
        collapseTags: true,
        options(_, fields) {
          return fields;
        },
        placeholder: 'Select Fields',
        field: 'concatFields',
        defaultValue: []
      }
    ],
    composeFn(field, params) {
      return `${params.separator},${field},${params.concatFields.join(',')}`;
    }
  },
  {
    label: 'LENGTH'
  },
  {
    label: 'LOWER'
  },
  {
    label: 'LTRIM'
  },
  {
    label: 'RTRIM'
  },
  {
    label: 'SUBSTRING',
    filters: [
      {
        type: 'number',
        label: 'position',
        placeholder: 'Separator_expr',
        field: 'pos',
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
    label: 'SUBSTR',
    filters: [
      {
        type: 'number',
        label: 'position',
        placeholder: 'Separator_expr',
        field: 'pos',
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
    label: 'UPPER'
  }
];
// 转换函数
export const ConversionFn = [
  // "CAST",
  'TO_ISO8601',
  'TO_JSON',
  'TO_UNIXTIMESTAMP'
];
// 时间和日期函数
export const DatetimeFN = ['NOW', 'TIMEDIFF', 'TIMETRUNCATE', 'TIMEZONE', 'TODAY'];
// 聚合函数
export const AggregationFn: TDFnType[] = [
  {
    label: 'APERCENTILE',
    applicableDataTypes: ['NUMBER'],
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
    ],
    composeFn(field, params) {
      return `${field},${params.p},"${params.algo_type}"`;
    }
  },
  {
    label: 'AVG',
    applicableDataTypes: ['NUMBER']
  },
  {
    label: 'COUNT'
  },
  {
    label: 'ELAPSED',
    applicableDataTypes: ['TIMESTAMP'],
    filters: [
      {
        type: 'select',
        label: 'time_unit',
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
        placeholder: 'time_unit',
        field: 'time_unit',
        defaultValue: ''
      }
    ]
  },
  {
    label: 'LEASTSQUARES',
    applicableDataTypes: ['NUMBER'],
    filters: [
      {
        type: 'number',
        label: 'start_val',
        placeholder: 'start_val',
        min: 0,
        field: 'start_val',
        defaultValue: 0
      },
      {
        type: 'number',
        label: 'step_val',
        placeholder: 'step_val',
        min: 0,
        field: 'step_val',
        defaultValue: 0
      }
    ]
  },
  {
    label: 'SPREAD',
    applicableDataTypes: ['NUMBER', 'TIMESTAMP']
  },
  {
    label: 'SUM',
    applicableDataTypes: ['NUMBER']
  },
  {
    label: 'HYPERLOGLOG'
  },
  {
    label: 'HISTOGRAM',
    applicableDataTypes: ['NUMBER'],
    filters: [
      {
        type: 'select',
        label: 'bin_type',
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
        placeholder: 'bin_type',
        field: 'bin_type',
        defaultValue: ''
      },
      {
        type: 'input',
        label: 'bin_description',
        placeholder: 'bin_description',
        field: 'bin_description',
        defaultValue: ''
      }
    ]
  },
  {
    label: 'PERCENTILE',
    applicableDataTypes: ['NUMBER'],
    filters: [
      {
        type: 'array',
        itemType: 'number',
        label: 'p',
        placeholder: 'p',
        min: 0,
        max: 100,
        field: 'p',
        defaultValue: []
      }
    ]
  }
];
// 选择函数
export const SelectorFn: TDFnType[] = [
  {
    label: 'BOTTOM',
    applicableDataTypes: ['NUMBER'],
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
    label: 'FIRST'
  },
  {
    label: 'INTERP',
    applicableDataTypes: ['NUMBER'],
    filters: [
      {
        type: 'select',
        label: 'ignore_null_values',
        placeholder: 'ignore_null_values',
        options: [
          {
            label: 'true',
            value: 1
          },
          {
            label: 'false',
            value: 0
          }
        ],
        field: 'ignore_null_values',
        defaultValue: 0
      }
    ]
  },
  {
    label: 'LAST'
  },
  {
    label: 'LAST_ROW'
  },
  {
    label: 'MAX',
    applicableDataTypes: ['NUMBER']
  },
  {
    label: 'MIN',
    applicableDataTypes: ['NUMBER']
  },
  {
    label: 'MODE'
  },
  {
    label: 'SAMPLE',
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
    label: 'TAIL',
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
        defaultValue: 1
      }
    ]
  },
  {
    label: 'TOP',
    applicableDataTypes: ['NUMBER'],
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
    label: 'UNIQUE'
  }
];

export interface TDFnType {
  label: string;
  applicableDataTypes?: string[];
  filters?: FnFilterItem[];
  composeFn?: (field: string, params: Recordable) => string;
}
export interface FnFilterItem {
  type: string;
  label: string;
  placeholder: string;
  field: string;
  defaultValue: any;
  options?: LabelValue[] | AnyFunction;
  min?: number;
  max?: number;
  itemType?: string;
  multiple?: boolean;
  collapseTags?: boolean;
}
// 时序数据特有函数
export const TimeSeriesFn: TDFnType[] = [
  {
    label: 'CSUM',
    applicableDataTypes: ['NUMBER']
  },
  {
    label: 'DERIVATIVE',
    applicableDataTypes: ['NUMBER'],
    filters: [
      {
        type: 'number',
        label: 'time_interval',
        placeholder: 'time_interval(s)',
        min: 1,
        max: 100,
        field: 'time_interval',
        defaultValue: 1
      },
      {
        type: 'select',
        label: 'ignore_negative',
        options: [
          {
            label: 'false',
            value: 0
          },
          {
            label: 'true',
            value: 1
          }
        ],
        placeholder: 'ignore_negative',
        field: 'ignore_negative',
        defaultValue: 0
      }
    ]
  },
  {
    label: 'DIFF',
    applicableDataTypes: ['NUMBER'],
    filters: [
      {
        type: 'select',
        label: 'ignore_negative',
        options: [
          {
            label: 'false',
            value: 0
          },
          {
            label: 'true',
            value: 1
          }
        ],
        placeholder: 'ignore_negative',
        field: 'ignore_negative',
        defaultValue: 0
      }
    ]
  },
  {
    label: 'IRATE',
    applicableDataTypes: ['NUMBER']
  },
  {
    label: 'MAVG',
    applicableDataTypes: ['NUMBER'],
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
    applicableDataTypes: ['NUMBER'],
    filters: [
      {
        type: 'select',
        label: 'oper',
        options: [
          {
            label: '<',
            value: 'LT'
          },
          {
            label: '>',
            value: 'GT'
          },
          {
            label: '<=',
            value: 'LE'
          },
          {
            label: '>=',
            value: 'GE'
          },
          {
            label: '==',
            value: 'EQ'
          }
        ],
        placeholder: 'oper',
        field: 'oper',
        defaultValue: 'LT'
      },
      {
        type: 'number',
        label: 'val',
        placeholder: 'val',
        min: 1,
        max: Infinity,
        field: 'val',
        defaultValue: 1
      }
    ]
  },
  {
    label: 'STATEDURATION',
    applicableDataTypes: ['NUMBER'],
    filters: [
      {
        type: 'select',
        label: 'oper',
        options: [
          {
            label: '<',
            value: 'LT'
          },
          {
            label: '>',
            value: 'GT'
          },
          {
            label: '<=',
            value: 'LE'
          },
          {
            label: '>=',
            value: 'GE'
          },
          {
            label: '==',
            value: 'EQ'
          }
        ],
        placeholder: 'oper',
        field: 'oper',
        defaultValue: 'LT'
      },
      {
        type: 'number',
        label: 'val',
        placeholder: 'val',
        min: 1,
        max: Infinity,
        field: 'val',
        defaultValue: 1
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
    label: 'TWA'
  }
];

// 系统信息函数
export const SystemFn = ['DATABASE', 'CLIENT_VERSION', 'SERVER_VERSION', 'SERVER_STATUS', 'CURRENT_USER', 'USER'];
// 流计算不支持的函数
export const StreamNotSupportFn = [
  'LEASTSQUARES',
  'PERCENTILE',
  'TOP',
  'BOTTOM',
  'ELAPSED',
  'INTERP',
  'DERIVATIVE',
  'IRATE',
  'TWA',
  'HISTOGRAM',
  'DIFF',
  'STATECOUNT',
  'STATEDURATION',
  'CSUM',
  'MAVG',
  'SAMPLE',
  'TAIL',
  'UNIQUE',
  'MODE'
];

function filterFNInclude(fnList: TDFnType[], type: string) {
  return fnList.filter(item => !item.applicableDataTypes || item.applicableDataTypes.includes(type));
}

// 流计算支持的函数
export const StreamSupportFnMap: Recordable<TDFnType[]> = {
  NUMBER: NumericFn.concat(filterFNInclude(SelectorFn, 'NUMBER'), filterFNInclude(AggregationFn, 'NUMBER'))
    .filter(item => !StreamNotSupportFn.includes(item.label))
    .sort((a, b) => a.label.localeCompare(b.label)),
  STRING: StringFn.concat(filterFNInclude(SelectorFn, 'STRING'), filterFNInclude(AggregationFn, 'STRING'))
    .filter(item => !StreamNotSupportFn.includes(item.label))
    .sort((a, b) => a.label.localeCompare(b.label)),
  AVGFN: AggregationFn.concat(SelectorFn, TimeSeriesFn)
    .filter(item => !StreamNotSupportFn.includes(item.label))
    .sort((a, b) => a.label.localeCompare(b.label))
};
// 流计算支持的函数列表
export const StreamSupportFnList = Object.keys(StreamSupportFnMap)
  .reduce((acc, key: string) => {
    const fnList = StreamSupportFnMap[key];
    fnList.forEach(item => {
      if (acc.every(ite => ite.label != item.label)) {
        acc.push(item);
      }
    });
    return acc;
  }, [] as TDFnType[])
  .sort((a, b) => a.label.localeCompare(b.label));

// 时间戳可使用的运算符
export const CompareOperator = ['>', '<', '>=', '<=', '!=', '='];
export const BooleanOperator = ['=='];
export const JsonOperator = ['Contains'];
export const StringOperator = ['LIKE', 'NOT LIKE'];

export const ConcatAndOperator = ['BETWEEN', 'NOT BETWEEN'];
export const ContainOperator = ['IN', 'NOT IN'];
export const GeneralOperator = [
  {
    label: 'IN'
  },
  {
    label: 'BETWEEN',
    include: ['TIMESTAMP', 'NUMBER']
  },
  {
    label: 'LIKE',
    include: ['STRING']
  },
  {
    label: 'NOT LIKE',
    include: ['STRING']
  },
  {
    label: 'NOT IN'
  },
  {
    label: 'NOT BETWEEN',
    include: ['TIMESTAMP', 'NUMBER']
  },
  {
    label: 'IS NULL'
  },
  {
    label: 'IS NOT NULL'
  }
];

function getGeneralFn(type: string) {
  return GeneralOperator.filter(item => !item.include || item.include.includes(type)).map(item => item.label);
}
export const NoValueOperator = ['IS NULL', 'IS NOT NULL'];

export const conditionMap = {
  TIMESTAMP: CompareOperator.concat(getGeneralFn('TIMESTAMP')),
  NUMBER: CompareOperator.concat(getGeneralFn('NUMBER')),
  STRING: CompareOperator.concat(getGeneralFn('STRING')),
  JSON: JsonOperator.concat(getGeneralFn('JSON')),
  BOOL: ['=', '!='].concat(getGeneralFn('BOOL'))
};
export const resultFnMap = {
  NUMBER: NumericFn,
  STRING: StringFn,
  AVGFN: AggregationFn
};

export const TwoVariableTableColumnType = ['DECIMAL'];
export const VariableTableColumnType = ['BINARY', 'NCHAR', 'VARCHAR', 'GEOMETRY', 'VARBINARY'];
export const VariableTableColumnTypeMaxLengthMap = {
  BINARY: 16374,
  NCHAR: 4093,
  VARCHAR: 16374,
  GEOMETRY: 16382,
  VARBINARY: 16382,
  DECIMAL: 38
};

export const TDengineStringType = ['VARCHAR', 'BINARY', 'NCHAR', 'GEOMETRY', 'VARBINARY'];
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
export const TDengineTimeUnit = [
  {
    label: t('date.nanoseconds'),
    value: 'b'
  },
  {
    label: t('date.microseconds'),
    value: 'u'
  },
  {
    label: t('date.milliseconds'),
    value: 'a'
  },
  {
    label: t('date.seconds'),
    value: 's'
  },
  {
    label: t('date.minutes'),
    value: 'm'
  },
  {
    label: t('date.hours'),
    value: 'h'
  },
  {
    label: t('date.days'),
    value: 'd'
  },
  {
    label: t('date.weeks'),
    value: 'w'
  },
  {
    label: t('date.months'),
    value: 'n'
  },
  {
    label: t('date.years'),
    value: 'y'
  }
];
export const DBParameters = [
  {
    name: 'buffer',
    type: 'number',
    alter: false,
    defaultValue: 32
  },
  {
    name: 'cachemodel',
    type: 'string',
    alter: true,
    defaultValue: 'none'
  },
  {
    name: 'cachesize',
    type: 'number',
    alter: true,
    defaultValue: 1
  },
  {
    name: 'comp',
    type: 'number',
    alter: false,
    defaultValue: 2
  },
  {
    name: 'duration',
    type: 'number',
    alter: false,
    defaultValue: '10d'
  },
  {
    name: 'wal_fsync_period',
    type: 'number',
    alter: true,
    defaultValue: 3000
  },
  {
    name: 'maxrows',
    type: 'number',
    alter: false,
    defaultValue: 4096
  },
  {
    name: 'minrows',
    type: 'number',
    alter: false,
    defaultValue: 100
  },
  {
    name: 'keep',
    type: 'number',
    alter: true,
    defaultValue: 3650
  },
  {
    name: 'pages',
    type: 'number',
    alter: false,
    defaultValue: 256
  },
  {
    name: 'pagesize',
    type: 'number',
    alter: false,
    defaultValue: 4
  },
  {
    name: 'precision',
    type: 'string',
    alter: false,
    defaultValue: 'ms'
  },
  {
    name: 'replica',
    type: 'number',
    alter: false,
    defaultValue: 1
  },
  {
    name: 'retentions',
    type: 'string',
    alter: false,
    defaultValue: ''
  },
  {
    name: 'strict',
    type: 'string',
    alter: false,
    defaultValue: 'off',
    version: '<=3.0.2.4'
  },
  {
    name: 'wal_level',
    type: 'number',
    alter: true,
    defaultValue: 1
  },
  {
    name: 'vgroups',
    type: 'number',
    alter: false,
    defaultValue: 4
  },
  {
    name: 'single_stable',
    type: 'number',
    alter: false,
    defaultValue: 0
  },
  {
    name: 'wal_retention_period',
    type: 'number',
    alter: false,
    defaultValue: 3600
  },
  {
    name: 'wal_retention_size',
    type: 'number',
    alter: false,
    defaultValue: 0
  },
  {
    name: 'wal_roll_period',
    type: 'number',
    alter: false,
    defaultValue: 0,
    version: '<=3.0.7.1'
  },
  {
    name: 'wal_segment_size',
    type: 'number',
    alter: false,
    defaultValue: 0,
    version: '<=3.0.7.1'
  },
  {
    name: 'stt_trigger',
    type: 'number',
    alter: false,
    defaultValue: 1,
    version: '>=3.0.5.0'
  },
  {
    name: 'tsdb_pagesize',
    type: 'number',
    alter: false,
    defaultValue: 4,
    version: '>=3.0.5.0'
  },
  {
    name: 'table_prefix',
    type: 'number',
    alter: false,
    version: '>=3.0.5.0'
  },
  {
    name: 'table_suffix',
    type: 'number',
    alter: false,
    version: '>=3.0.5.0'
  }
];

export const TDengineDataType = [
  'INT',
  'INT UNSIGNED',
  'BIGINT',
  'BIGINT UNSIGNED',
  'FLOAT',
  'DOUBLE',
  'SMALLINT',
  'SMALLINT UNSIGNED',
  'TINYINT',
  'TINYINT UNSIGNED',
  'TIMESTAMP',
  'BOOL',
  'BINARY',
  'VARCHAR',
  'NCHAR',
  'GEOMETRY',
  'VARBINARY',
  'DECIMAL',
  'BLOB'
];

export const TDengineSqlKeywrods = [
  'ABORT',
  'ACCOUNT',
  'ACCOUNTS',
  'ADD',
  'AFTER',
  'AGGREGATE',
  'ALIAS',
  'ALIVE',
  'ALL',
  'ALTER',
  'ANALYZE',
  'AND',
  'ANODE',
  'ANODES',
  'ANOMALY_WINDOW',
  'ANTI',
  'APPS',
  'ARBGROUPS',
  'ARROW',
  'AS',
  'ASC',
  'ASOF',
  'AT_ONCE',
  'ATTACH',
  'BALANCE',
  'BEFORE',
  'BEGIN',
  'BETWEEN',
  'BIGINT',
  'BIN',
  'BINARY',
  'BITAND',
  'BITAND',
  'BITNOT',
  'BITOR',
  'BLOB',
  'BLOCKS',
  'BNODE',
  'BNODES',
  'BOOL',
  'BOTH',
  'BUFFER',
  'BUFSIZE',
  'BWLIMIT',
  'BY',
  'CACHE',
  'CACHEMODEL',
  'CACHESIZE',
  'CASE',
  'CAST',
  'CHANGE',
  'CHILD',
  'CLIENT_VERSION',
  'CLUSTER',
  'COLON',
  'COLUMN',
  'COMMA',
  'COMMENT',
  'COMP',
  'COMPACT',
  'COMPACTS',
  'CONCAT',
  'CONFLICT',
  'CONNECTION',
  'CONNECTIONS',
  'CONNS',
  'CONSUMER',
  'CONSUMERS',
  'CONTAINS',
  'COPY',
  'COUNT',
  'COUNT_WINDOW',
  'CREATE',
  'CREATEDB',
  'CURRENT_USER',
  'DATABASE',
  'DATABASES',
  'DBS',
  'DECIMAL',
  'DEFERRED',
  'DELETE',
  'DELETE_MARK',
  'DELIMITERS',
  'DESC',
  'DESCRIBE',
  'DETACH',
  'DISTINCT',
  'DISTRIBUTED',
  'DIVIDE',
  'DNODE',
  'DNODES',
  'DOT',
  'DOUBLE',
  'DROP',
  'DURATION',
  'EACH',
  'ELSE',
  'ENABLE',
  'ENCRYPT_ALGORITHM',
  'ENCRYPT_KEY',
  'ENCRYPTIONS',
  'END',
  'EQ',
  'EVENT_WINDOW',
  'EVERY',
  'EXCEPT',
  'EXISTS',
  'EXPIRED',
  'EXPLAIN',
  'FAIL',
  'FHIGH',
  'FILE',
  'FILL',
  'FILL_HISTORY',
  'FIRST',
  'FLOAT',
  'FLOW',
  'FLUSH',
  'FOR',
  'FORCE',
  'FORCE_WINDOW_CLOSE',
  'FROM',
  'FROWTS',
  'FULL',
  'FUNCTION',
  'FUNCTIONS',
  'GE',
  'GEOMETRY',
  'GLOB',
  'GRANT',
  'GRANTS',
  'GROUP',
  'GT',
  'HAVING',
  'HEX',
  'HOST',
  'ID',
  'IF',
  'IGNORE',
  'ILLEGAL',
  'IMMEDIATE',
  'IMPORT',
  'IN',
  'INDEX',
  'INDEXES',
  'INITIALLY',
  'INNER',
  'INSERT',
  'INSTEAD',
  'INT',
  'INTEGER',
  'INTERSECT',
  'INTERVAL',
  'INTO',
  'IPTOKEN',
  'IROWTS',
  'IS',
  'IS_IMPORT',
  'ISFILLED',
  'ISNULL',
  'JLIMIT',
  'JOIN',
  'JSON',
  'KEEP',
  'KEEP_TIME_OFFSET',
  'KEY',
  'KILL',
  'LANGUAGE',
  'LAST',
  'LAST_ROW',
  'LE',
  'LEADER',
  'LEADING',
  'LEFT',
  'LICENCES',
  'LIKE',
  'LIMIT',
  'LINEAR',
  'LOCAL',
  'LOGS',
  'LP',
  'LSHIFT',
  'LT',
  'MACHINES',
  'MATCH',
  'MAX_DELAY',
  'MAXROWS',
  'MEDIUMBLOB',
  'MERGE',
  'META',
  'MINROWS',
  'MINUS',
  'MNODE',
  'MNODES',
  'MODIFY',
  'MODULES',
  'NCHAR',
  'NE',
  'NEXT',
  'NMATCH',
  'NONE',
  'NORMAL',
  'NOT',
  'NOTNULL',
  'NOW',
  'NULL',
  'NULL_F',
  'NULLS',
  'OF',
  'OFFSET',
  'ON',
  'ONLY',
  'OR',
  'ORDER',
  'OUTER',
  'OUTPUTTYPE',
  'PAGES',
  'PAGESIZE',
  'PARTITION',
  'PASS',
  'PAUSE',
  'PI',
  'PLUS',
  'PORT',
  'POSITION',
  'PPS',
  'PRECISION',
  'PREV',
  'PRIMARY',
  'PRIVILEGE',
  'PRIVILEGES',
  'QDURATION',
  'QEND',
  'QNODE',
  'QNODES',
  'QSTART',
  'QTAGS',
  'QTIME',
  'QUERIES',
  'QUERY',
  'QUESTION',
  'RAISE',
  'RAND',
  'RANGE',
  'RATIO',
  'READ',
  'RECURSIVE',
  'REDISTRIBUTE',
  'REM',
  'REPLACE',
  'REPLICA',
  'RESET',
  'RESTORE',
  'RESTRICT',
  'RESUME',
  'RETENTIONS',
  'REVOKE',
  'RIGHT',
  'ROLLUP',
  'ROW',
  'ROWTS',
  'RP',
  'RSHIFT',
  'S3_CHUNKPAGES',
  'S3_COMPACT',
  'S3_KEEPLOCAL',
  'SCHEMALESS',
  'SCORES',
  'SELECT',
  'SEMI',
  'SERVER_STATUS',
  'SERVER_VERSION',
  'SESSION',
  'SET',
  'SHOW',
  'SINGLE_STABLE',
  'SLASH',
  'SLIDING',
  'SLIMIT',
  'SMA',
  'SMALLINT',
  'SMIGRATE',
  'SNODE',
  'SNODES',
  'SOFFSET',
  'SPLIT',
  'STABLE',
  'STABLES',
  'STAR',
  'START',
  'STATE',
  'STATE_WINDOW',
  'STATEMENT',
  'STORAGE',
  'STREAM',
  'STREAMS',
  'STRICT',
  'STRING',
  'STT_TRIGGER',
  'SUBSCRIBE',
  'SUBSCRIPTIONS',
  'SUBSTR',
  'SUBSTRING',
  'SUBTABLE',
  'SYSINFO',
  'SYSTEM',
  'TABLE',
  'TABLE_PREFIX',
  'TABLE_SUFFIX',
  'TABLES',
  'TAG',
  'TAGS',
  'TBNAME',
  'THEN',
  'TIMES',
  'TIMESTAMP',
  'TIMEZONE',
  'TINYINT',
  'TO',
  'TODAY',
  'TOPIC',
  'TOPICS',
  'TRAILING',
  'TRANSACTION',
  'TRANSACTIONS',
  'TRIGGER',
  'TRIM',
  'TSDB_PAGESIZE',
  'TSERIES',
  'TSMA',
  'TSMAS',
  'TTL',
  'UNION',
  'UNSAFE',
  'UNSIGNED',
  'UNTREATED',
  'UPDATE',
  'USE',
  'USER',
  'USERS',
  'USING',
  'VALUE',
  'VALUE_F',
  'VALUES',
  'VARBINARY',
  'VARCHAR',
  'VARIABLE',
  'VARIABLES',
  'VERBOSE',
  'VGROUP',
  'VGROUPS',
  'VIEW',
  'VIEWS',
  'VNODE',
  'VNODES',
  'WAL',
  'WAL_FSYNC_PERIOD',
  'WAL_LEVEL',
  'WAL_RETENTION_PERIOD',
  'WAL_RETENTION_SIZE',
  'WAL_ROLL_PERIOD',
  'WAL_SEGMENT_SIZE',
  'WATERMARK',
  'WDURATION',
  'WEND',
  'WHEN',
  'WHERE',
  'WINDOW',
  'WINDOW_CLOSE',
  'WINDOW_OFFSET',
  'WITH',
  'WRITE',
  'WSTART',
  '_C0',
  '_IROWTS',
  '_QDURATION',
  '_QEND',
  '_QSTART',
  '_ROWTS',
  '_WDURATION',
  '_WEND',
  '_WSTART'
];

export const DBCustomedFiled = [
  'parent',
  'node-key',
  'typeName',
  'privileges',
  'databaseId',
  'databaseName',
  'databaseAccessType'
];
export const TDengineFnReverseGroup = ['CONCAT_WS'];
export const DownloadZhUrl = 'https://downloads.taosdata.com';
export const DownloadEnUrl = 'https://downloads.tdengine.com';

export const HIDEDB = ['information_schema', 'performance_schema'];
export const DB_FIELDS: Recordable<Recordable> = {
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
