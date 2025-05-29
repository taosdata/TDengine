export interface TableRow {
  Name: string;
  Type: string;
  exprname: string;
  maptype: [string, string];
  Expression: string;
  PrimaryKey?: boolean;
  dataRange?: (bigint | number)[];
  dataType?: string;
}

export interface TransformerState {
  csvParser: csvParser | null;
  transformExtractParseData: TransformExtractParseDataType | null;
  csvTransformerParser: CsvTransformerParserType | null;
  transformerFilterParseData: TransformerFilterParseDataType | null;
  transformerMapCloumns: TransformerMapCloumnsType[];
  transformerParserData: TransformerfullparamsType | TransformerSpbfullparamsType | null;
  transformColumnIdentify: [];
  csvTransformerlocalCols: string[]; //csv无头部时候的自定义列
  splitExpresList: SplitExpresListType | null; //transformer的split;
  convertExpresList: ConvertExpresListType | null;
  mappingjoin: ''; //mapping时候映射值是join时候的
  definitions: [];
  topParse: TopParseType | SpbTopParseType | null;
  transformResultTable: any[];
  createStWithoutDB: number;
  transformTableHeight: number;
  transformerfullparams: TransformerfullparamsType | TransformerSpbfullparamsType | null;
  transResultName: string;
  historianechodata: null;
  s_model: Recordable;
  limitOffset: number;
  showResultTb: boolean;
  resultTbTitle: string;
  activeColumns: string[]; // 转换拆分出来的新字段
  resultCurrentPage: 1;
  stbDefaultColumns: Record<string, any>[]; // transfrom 创建超级表时默认的列
}

export interface CsvFileConfigType {
  fileurl: string;
  file_pattern: string;
  new_file_notify: boolean | string;
  notify_interval: string | number;
  sort: string;
  keep_processed_files: boolean | string;
}

interface csvParser {
  input: Recordable[];
}
interface TransformerMapCloumnsType {
  value: string;
  label: string;
  children: Recordable[];
  [x: string]: any;
}

export interface TransformExtractParseDataType {
  extract: Recordable;
}

export interface ParseType {
  payload?: Recordable;
  value?: {
    json?: string;
  };
  [x: string]: any;
}

export interface TopParseType {
  input: Recordable[];
  parser: {
    mutate?: Recordable[];
    parse?: ParseType;
  };
}

export interface SpbTopParseType {
  samples: Recordable[];
  parser: {
    mutate?: Recordable[];
    parse?: ParseType;
  };
}

export interface CsvTransformerParserType {
  inputList: Recordable[];
  msgBody: string;
  columns?: string[];
}

export interface SplitExpresListType {
  n: number | string;
  sep: string;
  names: string[] | string;
  [key: string]: any;
}

export interface ConvertExpresListType {
  rule: string,
  name: string,
  [key: string]: any;
}

interface TransformerFilterParseDataType {
  filter: string;
}

export interface TransformerfullparamsType {
  parser: {
    global?: Recordable;
    parse: ParseType;
    model: Recordable;
    mutate: Recordable[];
    s_model?: Recordable;
  };
  input: Recordable[];
  format: {
    pageCount: number;
    pageSize: number;
    currentPage: number;
  };
}

export interface TransformerSpbfullparamsType {
  parser: {
    global?: Recordable;
    parse: ParseType;
    model: Recordable;
    mutate: Recordable[];
    s_model?: Recordable;
  };
  samples: Recordable[];
  format: {
    pageCount: number;
    pageSize: number;
    currentPage: number;
  };
}
