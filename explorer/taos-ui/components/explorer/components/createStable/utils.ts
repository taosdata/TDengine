import { CreateStableForm } from '../props';
import { addStrBackquote, escapeSpecialChar, composeType } from 'utils/tdengine';
// 版本大于等于 3.3.0.0 版本的新字段
export const parmaryKeyType = [
  {
    label: 'INT',
    value: 'INT'
  },
  {
    label: 'INT UNSIGNED',
    value: 'INT UNSIGNED'
  },
  {
    label: 'BIGINT',
    value: 'BIGINT'
  },
  {
    label: 'BIGINT UNSIGNED',
    value: 'BIGINT UNSIGNED'
  },
  {
    label: 'VARCHAR',
    value: 'VARCHAR'
  }
];

export const storageCompression = {
  // TINYINT/TINYINT UNSIGNED/SMALLINT/SMALLINT UNSIGNED/INT/INT UNSIGNED
  groupOne: {
    encodeList: [
      {
        label: 'simple8b',
        value: 'simple8b'
      }
    ],
    compressList: [
      {
        label: 'lz4',
        value: 'lz4'
      },
      {
        label: 'zlib',
        value: 'zlib'
      },
      {
        label: 'zstd',
        value: 'zstd'
      },
      {
        label: 'xz',
        value: 'xz'
      }
    ],
    includes: ['TINYINT', 'TINYINT UNSIGNED', 'SMALLINT', 'SMALLINT UNSIGNED', 'INT', 'INT UNSIGNED'],
    defaultEncode: 'simple8b',
    defaultCompress: 'lz4'
  },
  // BIGINT/TIMESTAMP/BIGINT UNSIGNED
  groupTwo: {
    encodeList: [
      {
        label: 'simple8b',
        value: 'simple8b'
      },
      {
        label: 'delta-i',
        value: 'delta-i'
      }
    ],
    compressList: [
      {
        label: 'lz4',
        value: 'lz4'
      },
      {
        label: 'zlib',
        value: 'zlib'
      },
      {
        label: 'zstd',
        value: 'zstd'
      },
      {
        label: 'xz',
        value: 'xz'
      }
    ],
    includes: ['BIGINT', 'TIMESTAMP', 'BIGINT UNSIGNED'],
    defaultEncode: 'delta-i',
    defaultCompress: 'lz4'
  },
  // FLOAT/DOUBLE
  groupThree: {
    encodeList: [
      {
        label: 'delta-d',
        value: 'delta-d'
      }
    ],
    compressList: [
      {
        label: 'lz4',
        value: 'lz4'
      },
      {
        label: 'zlib',
        value: 'zlib'
      },
      {
        label: 'zstd',
        value: 'zstd'
      },
      {
        label: 'tsz',
        value: 'tsz'
      }
    ],
    includes: ['FLOAT', 'DOUBLE'],
    defaultEncode: 'delta-d',
    defaultCompress: 'lz4'
  },
  // BINARY /NCHAR/ VARCHAR
  groupFour: {
    encodeList: [
      {
        label: 'disabled',
        value: 'disabled'
      }
    ],
    compressList: [
      {
        label: 'lz4',
        value: 'lz4'
      },
      {
        label: 'zlib',
        value: 'zlib'
      },
      {
        label: 'zstd',
        value: 'zstd'
      },
      {
        label: 'xz',
        value: 'xz'
      }
    ],
    includes: ['BINARY', 'NCHAR', 'VARCHAR', 'VARBINARY', 'GEOMETRY'],
    defaultEncode: 'disabled',
    defaultCompress: 'lz4'
  },
  // BOOL
  groupFive: {
    encodeList: [
      {
        label: 'bit-packing',
        value: 'bit-packing'
      }
    ],
    compressList: [
      {
        label: 'lz4',
        value: 'lz4'
      },
      {
        label: 'zlib',
        value: 'zlib'
      },
      {
        label: 'zstd',
        value: 'zstd'
      },
      {
        label: 'xz',
        value: 'xz'
      }
    ],
    includes: ['BOOL'],
    defaultEncode: 'bit-packing',
    defaultCompress: 'lz4'
  },
  groupSix: {
    encodeList: [
      {
        label: 'delta-i',
        value: 'delta-i'
      }
    ],
    compressList: [
      {
        label: 'lz4',
        value: 'lz4'
      },
      {
        label: 'zlib',
        value: 'zlib'
      },
      {
        label: 'zstd',
        value: 'zstd'
      },
      {
        label: 'xz',
        value: 'xz'
      }
    ],
    includes: ['TIMESTAMP'],
    defaultEncode: 'delta-i',
    defaultCompress: 'lz4'
  },
  empty: {
    encodeList: [],
    compressList: [],
    defaultEncode: '',
    defaultCompress: ''
  }
};

export const levelList = [
  {
    label: 'high',
    value: 'high'
  },
  {
    label: 'low',
    value: 'low'
  },
  {
    label: 'medium',
    value: 'medium'
  }
];
export const groupOne = ['TINYINT', 'TINYINT UNSIGNED', 'SMALLINT', 'SMALLINT UNSIGNED', 'INT', 'INT UNSIGNED'];
export const groupTwo = ['BIGINT', 'BIGINT UNSIGNED'];
export const groupThree = ['FLOAT', 'DOUBLE'];
export const groupFour = ['BINARY', 'NCHAR', 'VARCHAR', 'VARBINARY', 'GEOMETRY', 'DECIMAL'];
export const groupFive = ['BOOL'];
export const groupSix = ['TIMESTAMP'];

export const type_default_version_gte_3300 = {
  TIMESTAMP: {
    encode: 'delta-i',
    compress: 'lz4',
    level: 'medium',
    primaryKey: false
  },
  INT: {
    encode: 'simple8b',
    compress: 'lz4',
    level: 'medium',
    primaryKey: false
  }
};

export function getStbEncodeAndCompressListByType(type: string) {
  if (!type) return storageCompression.empty;
  if (groupOne.includes(type)) {
    return storageCompression.groupOne;
  } else if (groupTwo.includes(type)) {
    return storageCompression.groupTwo;
  } else if (groupThree.includes(type)) {
    return storageCompression.groupThree;
  } else if (groupFour.findIndex(item => type.startsWith(item)) !== -1) {
    return storageCompression.groupFour;
  } else if (groupFive.includes(type)) {
    return storageCompression.groupFive;
  } else {
    return storageCompression.groupSix;
  }
}
export function getStbDefaultEncodeAndCompressByType(type: string) {
  const config = getStbEncodeAndCompressListByType(type);
  return {
    encode: config.defaultEncode,
    compress: config.defaultCompress,
    level: 'medium'
  };
}

export function generateCreateStbSql(data: CreateStableForm, dbName: string) {
  const { name, columns, tags } = data;
  `CREATE STABLE \`${dbName}\`.${name} (${columns
    .map(
      item =>
        `${addStrBackquote(escapeSpecialChar(item.field))} ${composeType(item)}${item.encode ? ' ENCODE ' + `'${item.encode}'` : ''}${item.compress ? ' COMPRESS ' + `'${item.compress}'` : ''}${item.level ? ' LEVEL ' + `'${item.level}'` : ''
        }${item.primaryKey ? ' PRIMARY KEY' : ''}`
    )
    .join(
      ','
    )}) TAGS (${tags.map(item => `${addStrBackquote(escapeSpecialChar(item.field))} ${composeType(item)}`).join(',')});`;
}
