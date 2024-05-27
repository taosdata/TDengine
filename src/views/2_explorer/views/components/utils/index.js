export const groupOne = ['TINYINT','TINYINT UNSIGNED','SMALLINT','SMALLINT UNSIGNED','INT','INT UNSIGNED'];
export const groupTwo = ['BIGINT','BIGINT UNSIGNED']
export const groupThree = ['FLOAT','DOUBLE'];
export const groupFour = ['BINARY','NCHAR','VARCHAR','VARBINARY','GEOMETRY'];
export const groupFive = ['BOOL'];
export const groupSix = ['TIMESTAMP'];

export const dataType = [
  {
    label: "INT",
    value: "INT",
    supportDatatype: 'groupOne'
  },
  {
    label: "INT UNSIGNED",
    value: "INT UNSIGNED",
  },
  {
    label: "BIGINT",
    value: "BIGINT",
  },
  {
    label: "BIGINT UNSIGNED",
    value: "BIGINT UNSIGNED",
  },
  {
    label: "FLOAT",
    value: "FLOAT",
  },
  {
    label: "DOUBLE",
    value: "DOUBLE",
  },
  {
    label: "SMALLINT",
    value: "SMALLINT",
  },
  {
    label: "SMALLINT UNSIGNED",
    value: "SMALLINT UNSIGNED",
  },
  {
    label: "TINYINT",
    value: "TINYINT",
  },
  {
    label: "TINYINT UNSIGNED",
    value: "TINYINT UNSIGNED",
  },
  {
    label: "BOOL",
    value: "BOOL",
  },
  {
    label: "TIMESTAMP",
    value: "TIMESTAMP",
  },
  {
    label:'VARCHAR',
    value:'VARCHAR'
  },
  {
    label:'NCHAR',
    value:'NCHAR'
  },
  {
    label:'VARBINARY',
    value:'VARBINARY'
  },
  {
    label:'GEOMETRY',
    value:'GEOMETRY'
  },
  // {
  //   label:'BINARY',
  //   value:'BINARY'
  // }
];

export const tagType = [
  {
    label: "INT",
    value: "INT",
  },
  {
    label: "INT UNSIGNED",
    value: "INT UNSIGNED",
  },
  {
    label: "BIGINT",
    value: "BIGINT",
  },
  {
    label: "BIGINT UNSIGNED",
    value: "BIGINT UNSIGNED",
  },
  {
    label: "FLOAT",
    value: "FLOAT",
  },
  {
    label: "DOUBLE",
    value: "DOUBLE",
  },
  {
    label: "SMALLINT",
    value: "SMALLINT",
  },
  {
    label: "SMALLINT UNSIGNED",
    value: "SMALLINT UNSIGNED",
  },
  {
    label: "TINYINT",
    value: "TINYINT",
  },
  {
    label: "TINYINT UNSIGNED",
    value: "TINYINT UNSIGNED",
  },
  {
    label: "BOOL",
    value: "BOOL",
  },
  {
    label: "TIMESTAMP",
    value: "TIMESTAMP",
  },
  {
    label:'VARCHAR',
    value:'VARCHAR'
  },
  {
    label:'NCHAR',
    value:'NCHAR'
  },
  {
    label:'JSON',
    value:'JSON'
  },
  {
    label:'VARBINARY',
    value:'VARBINARY'
  },
  {
    label:'GEOMETRY',
    value:'GEOMETRY'
  },
];

export const parmaryKeyType = [
  {
    label: "INT",
    value: "INT",
  },
  {
    label: "INT UNSIGNED",
    value: "INT UNSIGNED",
  },
  {
    label: "BIGINT",
    value: "BIGINT",
  },
  {
    label: "BIGINT UNSIGNED",
    value: "BIGINT UNSIGNED",
  },
  {
    label:'VARCHAR',
    value:'VARCHAR'
  },
]

export const storageCompression = {
  // TINYINT/TINYINT UNSIGNED/SMALLINT/SMALLINT UNSIGNED/INT/INT UNSIGNED
  groupOne: {
    encodeList: [
      {
        label: "simple8b",
        value: "simple8b",
      },
    ],
    compressList: [
      {
        label: "lz4",
        value: "lz4",
      },
      {
        label: "zlib",
        value: "zlib",
      },
      {
        label: "zstd",
        value: "zstd",
      },
      {
        label: "xz",
        value: "xz",
      },
    ],
    defaultEncode: 'simple8b',
    defaultCompress: 'lz4',
  },
  // BIGINT/TIMESTAMP/BIGINT UNSIGNED
  groupTwo: {
    encodeList: [
      {
        label: "simple8b",
        value: "simple8b",
      },
      {
        label: "delta-i",
        value: "delta-i",
      }
    ],
    compressList: [
      {
        label: "lz4",
        value: "lz4",
      },
      {
        label: "zlib",
        value: "zlib",
      },
      {
        label: "zstd",
        value: "zstd",
      },
      {
        label: "xz",
        value: "xz",
      },
    ],
    defaultEncode: 'delta-i',
    defaultCompress: 'lz4',
  },
  // FLOAT/DOUBLE
  groupThree: {
    encodeList: [
      {
        label: "delta-d",
        value: "delta-d",
      },
    ],
    compressList: [
      {
        label: "lz4",
        value: "lz4",
      },
      {
        label: "zlib",
        value: "zlib",
      },
      {
        label: "zstd",
        value: "zstd",
      },
      {
        label: "xz",
        value: "xz",
      },
      {
        label: "tsz",
        value: "tsz",
      },
    ],
    defaultEncode: 'delta-d',
    defaultCompress: 'lz4',
  },
  // BINARY /NCHAR/ VARCHAR
  groupFour: {
    encodeList: [
      {
        label: "disabled",
        value: "disabled",
      }
    ],
    compressList: [
      {
        label: "lz4",
        value: "lz4",
      },
      {
        label: "zlib",
        value: "zlib",
      },
      {
        label: "zstd",
        value: "zstd",
      },
      {
        label: "xz",
        value: "xz",
      },
    ],
    defaultEncode: 'disabled',
    defaultCompress: 'lz4',
  },
  // BOOL
  groupFive: {
    encodeList: [
      {
        label: "bit-packing",
        value: "bit-packing",
      },
    ],
    compressList: [
      {
        label: "lz4",
        value: "lz4",
      },
      {
        label: "zlib",
        value: "zlib",
      },
      {
        label: "zstd",
        value: "zstd",
      },
      {
        label: "xz",
        value: "xz",
      },
    ],
    defaultEncode: 'bit-packing',
    defaultCompress: 'lz4',
  },
  groupSix: {
    encodeList: [
      {
        label: "delta-i",
        value: "delta-i",
      }
    ],
    compressList: [
      {
        label: "lz4",
        value: "lz4",
      },
      {
        label: "zlib",
        value: "zlib",
      },
      {
        label: "zstd",
        value: "zstd",
      },
      {
        label: "xz",
        value: "xz",
      },
    ],
    defaultEncode: 'delta-i',
    defaultCompress: 'lz4',
  },
  empty: {
    encodeList: [],
    compressList: [],
    defaultEncode: '',
    defaultCompress: '',
  }
}

export const levelList = [
  {
    label: "high",
    value: "high",
  },
  {
    label: "low",
    value: "low",
  },
  {
    label: "medium",
    value: "medium",
  },
]