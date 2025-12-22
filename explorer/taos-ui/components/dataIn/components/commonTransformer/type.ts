export interface TableRow {
  Name: string;
  Type: string;
  exprname: string;
  maptype: [string, string];
  // 表达式在部分模式下（如 sum/join）会多选，使用数组承载
  Expression: string | string[];
  PrimaryKey?: boolean;
  dataRange?: (bigint | number)[];
  dataType?: string;
  // 映射默认值（不同类型控件可能传入字符串或数字）
  default?: string | number | null;
  // 默认值校验错误信息
  defaultValueError?: string;
  // join 模式下的附加参数
  joinwith?: string;
}

export interface TransformerState {
  csvParser: csvParser | null;
  transformExtractParseData: TransformExtractParseDataType | null;
  csvTransformerParser: CsvTransformerParserType | null;
  transformerFilterParseData: TransformerFilterParseDataType | null;
  transformerMapColumns: TransformerMapColumnsType[];
  transformerParserData: TransformerfullparamsType | TransformerSpbfullparamsType | null;
  transformColumnIdentify: [];
  csvTransformerlocalCols: string[]; //csv无头部时候的自定义列
  splitExpressList: SplitExpressListType | null; //transformer的split;
  convertExpressList: ConvertExpressListType | null;
  jsonExtractListType: JsonParseExtractType | null;
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
  stbDefaultColumns: Record<string, any>[]; // transform 创建超级表时默认的列
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
interface TransformerMapColumnsType {
  value: string;
  label: string;
  children: Recordable[];
  [x: string]: any;
}

export interface TransformExtractParseDataType {
  extract: Recordable;
}

export interface ParseType {
  payload?: {
    json?: string | string[] | Recordable[] | any;
    [x: string]: any;
  };
  value?: {
    json?: string | string[] | Recordable[];
    [x: string]: any;
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

export interface SplitExpressListType {
  n: number | string;
  sep: string;
  names: string[] | string;
  [key: string]: any;
}

export interface ConvertExpressListType {
  rule: string;
  name: string;
  [key: string]: any;
}

export interface JsonParseExtractType {
  depth: number;
  keep: boolean;
  expression: any;
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
