export const groupOne = ['TINYINT','TINYINT UNSIGNED','SMALLINT','SMALLINT','UNSIGNED','INT','INT UNSIGNED'];
export const groupTwo = ['BIGINT','TIMESTAMP','BIGINT UNSIGNED']
export const groupThree = ['FLOAT','DOUBLE'];
export const groupFour = ['BINARY','NCHAR','VARCHAR'];
export const groupFive = ['BOOL'];
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
  }
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
    ]
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
    ]
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
        label: "tsz",
        value: "tsz",
      },
    ]
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
    ]
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
    ]
  },
  
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