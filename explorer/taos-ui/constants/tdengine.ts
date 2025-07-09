import { t } from 'locales';
// 数学函数
export const NumbericFn: TDFnType[] = [
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
    label: 'SUBSTRING/SUBSTR',
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
export const CoversionFn = [
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
            label: 'millsecond',
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
            label: 'millsecond',
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
  NUMBER: NumbericFn.concat(filterFNInclude(SelectorFn, 'NUMBER'), filterFNInclude(AggregationFn, 'NUMBER'))
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
  NUMBER: NumbericFn,
  STRING: StringFn,
  AVGFN: AggregationFn
};

export const TwoVariableTableColumnType = ['DECIMAL']
export const VariableTableColumnType = ['BINARY', 'NCHAR', 'VARCHAR', 'GEOMETRY', 'VARBINARY'];
export const VariableTableColumnTypeMaxLenthMap = {
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
    label: t('date.nanosecond'),
    value: 'b'
  },
  {
    label: t('date.microsecond'),
    value: 'u'
  },
  {
    label: t('date.millsecond'),
    value: 'a'
  },
  {
    label: t('date.second'),
    value: 's'
  },
  {
    label: t('date.minute'),
    value: 'm'
  },
  {
    label: t('date.hour'),
    value: 'h'
  },
  {
    label: t('date.day'),
    value: 'd'
  },
  {
    label: t('date.week'),
    value: 'w'
  },
  {
    label: t('date.month'),
    value: 'n'
  },
  {
    label: t('date.year'),
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
    defaultValue: '50d'
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
    defaultValue: 1
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
  'DECIMAL'
];

export const TDengineSqlKeywrods = [
  'ABORT',
  'ABS',
  'ACCOUNT',
  'ACCOUNTS',
  'ACOS',
  'ADD',
  'AFTER',
  'ALL',
  'ALTER',
  'AND',
  'APERCENTILE',
  'AS',
  'ASC',
  'ASIN',
  'ATAN',
  'ATTACH',
  'AVG',
  'BEFORE',
  'BEGIN',
  'BETWEEN',
  'BIGINT',
  'BINARY',
  'BITAND',
  'BITNOT',
  'BITOR',
  'BLOCKS',
  'BOOL',
  'BOTTOM',
  'BY',
  'CACHE',
  'CACHELAST',
  'CASCADE',
  'CAST',
  'CEIL',
  'CHANGE',
  'CHAR_LENGTH',
  'CLIENT_VERSION',
  'CLUSTER',
  'COLON',
  'COLUMN',
  'COMMA',
  'COMP',
  'COMPACT',
  'CONCAT',
  'CONCAT_WS',
  'CONFLICT',
  'CONNECTION',
  'CONNECTIONS',
  'CONNS',
  'COPY',
  'COS',
  'COUNT',
  'CREATE',
  'CSUM',
  'CTIME',
  'CURRENT_USER',
  'DATABASE',
  'DATABASES',
  'DAYS',
  'DBS',
  'DEFERRED',
  'DELETE',
  'DELIMITERS',
  'DERIVATIVE',
  'DESC',
  'DESCRIBE',
  'DETACH',
  'DIFF',
  'DISTINCT',
  'DIVIDE',
  'DNODE',
  'DNODES',
  'DOT',
  'DOUBLE',
  'DROP',
  'ELAPSED',
  'END',
  'EQ',
  'EXISTS',
  'EXPLAIN',
  'FAIL',
  'FILE',
  'FILL',
  'FIRST',
  'FLOAT',
  'FLOOR',
  'FOR',
  'FROM',
  'FSYNC',
  'GE',
  'GLOB',
  'GRANTS',
  'GROUP',
  'GT',
  'HAVING',
  'HISTOGRAM',
  'HYPERLOGLOG',
  'ID',
  'IF',
  'IGNORE',
  'IMMEDIA',
  'IMPORT',
  'IN',
  'INITIAL',
  'INSERT',
  'INSTEAD',
  'INT',
  'INTEGER',
  'INTERP',
  'INTERVAL',
  'INTO',
  'IRATE',
  'IS',
  'IS NULL',
  'JOIN',
  'KEEP',
  'KEY',
  'KILL',
  'LAST',
  'LAST_ROW',
  'LE',
  'LEASTSQUARES',
  'LENGTH',
  'LIKE',
  'LIMIT',
  'LINEAR',
  'LOCAL',
  'LOG',
  'LOWER',
  'LP',
  'LSHIFT',
  'LT',
  'LTRIM',
  'MATCH',
  'MAVG',
  'MAX',
  'MAXROWS',
  'MIN',
  'MINROWS',
  'MINUS',
  'MNODES',
  'MODE',
  'MODIFY',
  'MODULES',
  'NE',
  'NONE',
  'NOT',
  'NOT NULL',
  'NOW',
  'NULL',
  'OF',
  'OFFSET',
  'OR',
  'ORDER',
  'PARTITION',
  'PASS',
  'PERCENTILE',
  'PLUS',
  'POW',
  'PPS',
  'PRECISION',
  'PREV',
  'PRIVILEGE',
  'QTIME',
  'QUERIE',
  'QUERY',
  'QUORUM',
  'RAISE',
  'REM',
  'REPLACE',
  'REPLICA',
  'RESET',
  'RESTRIC',
  'ROUND',
  'ROW',
  'RP',
  'RSHIFT',
  'RTLIM',
  'SAMPLE',
  'SCORES',
  'SELECT',
  'SEMI',
  'SERVER_STATUS',
  'SERVER_VERSION',
  'SESSION',
  'SET',
  'SHOW',
  'SIN',
  'SLASH',
  'SLIDING',
  'SLIMIT',
  'SMALLIN',
  'SOFFSET',
  'SPREAD',
  'SQRT',
  'STAR',
  'STATE',
  'STATECOUNT',
  'STATEDURATION',
  'STATEMENT',
  'STATE_WI',
  'STORAGE',
  'STREAM',
  'STREAMS',
  'STRING',
  'STable',
  'STableS',
  'SUM',
  'SYNCDB',
  'TABLE',
  'TABLES',
  'TAG',
  'TAGS',
  'TAIL',
  'TAN',
  'TBNAME',
  'TIMEDIFF',
  'TIMES',
  'TIMESTAMP',
  'TIMETRUNCATE',
  'TIMEZONE',
  'TINYINT',
  'TODAY',
  'TOP',
  'TOPIC',
  'TOPICS',
  'TO_ISO8601',
  'TO_JSON',
  'TO_UNIXTIMESTAMP',
  'TRIGGER',
  'TSERIES',
  'TWA',
  'UMINUS',
  'UNION',
  'UNIQUE',
  'UNSIGNED',
  'UPDATE',
  'UPLUS',
  'UPPER',
  'USE',
  'USER',
  'USERS',
  'USING',
  'VALUES',
  'VARIABLE',
  'VARIABLES',
  'VGROUPS',
  'VIEW',
  'VNODES',
  'WAL',
  'WHERE',
  '_C0',
  '_QDURATION',
  '_QSTART',
  '_QSTOP',
  '_WDURATION',
  '_WSTART',
  '_WSTOP'
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
