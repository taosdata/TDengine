import { TransformerState } from './type';
export const VariableTableColumnType = ['BINARY', 'NCHAR', 'VARCHAR', 'VARBINARY', 'GEOMETRY'];

// 将模版解析的结果转换成和用sql查询返回的数据结构一致
export function convert(data: any) {
  return [].concat(data).flatMap((item: Recordable) => {
    // 提取 tags 中所有字段名
    const tags = item.tags.map((tag: { name: string }) => tag.name);
    const allData = item.columns.concat(item.tags);
    return allData.map((col: { name: string; type: string; length: null | number }) => {
      const name = col.name.replace(/`/g, ''); // 去除反引号
      const type = col.type;
      const length = col.length !== null ? col.length : '';
      const isTag = tags.includes(name) ? 'TAG' : '';

      // 返回字段数据，带 TAG 的字段加上 TAG，其他字段正常返回
      return length === '' ? [name, type, length] : [name, type, length, isTag].filter(Boolean); // 去除空值
    });
  });
}

export function extractAllProperties(sampleData: string, deep: number | undefined) {
  // 1. Remove all quoted strings，避免字符串中包含{}导致提取出错
  const json_list = getExampleList(sampleData, true);
  const jsonObject = {};
  for (let i = 0; i < json_list.length; i++) {
    const json = json_list[i];
    if (Array.isArray(json)) {
      for (let j = 0; j < json.length; j++) {
        Object.assign(jsonObject, json[j]);
      }
    } else {
      Object.assign(jsonObject, json);
    }
  }
  return getAllProperties(jsonObject, deep);
}

function getAllProperties(obj: { [x: string]: any }, deep: number | undefined) {
  const properties: string[] = [];

  function traverse(prefix: string, obj: { [x: string]: any }, my_deep: number) {
    for (const key in obj) {
      if (my_deep < Number(deep) && typeof obj[key] === 'object' && !Array.isArray(obj[key])) {
        traverse(`${prefix}["${key}"]`, obj[key], my_deep + 1);
      } else {
        properties.push(`${prefix}["${key}"]`);
      }
    }
  }

  traverse('$', obj, 0);
  return properties;
}

// 获取示例数据字符串列表[]
// 返回字符串，则为错误信息
export function getExampleList(demo_data: string, parsed?: boolean | undefined) {
  const demo_string = (demo_data || '').trim();
  const demo_string_arr = [];
  if (demo_string.startsWith('[') && demo_string.endsWith(']')) {
    const arr_list = demo_string.replace(/\]\s*\[/g, ']&$[').split('&$');
    let total = 0;
    for (let i = 0; i < arr_list.length; i++) {
      try {
        const item_parsed = JSON.parse(arr_list[i]);
        total += item_parsed.length;
        if (parsed) {
          demo_string_arr.push(item_parsed);
        } else {
          demo_string_arr.push(arr_list[i]);
        }
      } catch (err: any) {
        err.lineNumber = i + 1;
        throw err;
      }
      if (total >= 100) {
        return demo_string_arr;
      }
    }
  } else if (demo_string.startsWith('{') && demo_string.endsWith('}')) {
    const obj_list = demo_string.replace(/\}\s*\{/g, '}&${').split('&$');
    for (let i = 0; i < obj_list.length; i++) {
      if (i >= 100) {
        return demo_string_arr;
      }
      try {
        if (parsed) {
          const item_parsed = JSON.parse(obj_list[i].replace(/\n/g, ''));
          demo_string_arr.push(item_parsed);
        } else {
          demo_string_arr.push(obj_list[i].replace(/\n/g, ''));
        }
      } catch (err: any) {
        err.lineNumber = i + 1;
        throw err;
      }
    }
  } else {
    throw 'dataIn.transformer.jsontip';
  }
  return demo_string_arr;
}

export function validateJsonKeys(data: string[]): void {
  function checkKeys(obj: any, path: string = ''): void {
    for (const key in obj) {
      const fullPath = path ? `${path}.${key}` : key;
      if (key.includes('.')) {
        throw new Error(`Invalid key "${fullPath}" cannot contain character "." `);
      }
      if (typeof obj[key] === 'object' && obj[key] !== null) {
        checkKeys(obj[key], fullPath);
      }
    }
  }

  data.forEach((jsonString, index) => {
    try {
      const obj = JSON.parse(jsonString);
      checkKeys(obj);
    } catch (err: any) {
      throw new Error(`Error in JSON entry ${index + 1}: ${err.message}`);
    }
  });
}

export function checkParseData(data: Recordable) {
  const mutateRules = data.parser?.mutate;
  if (!mutateRules) {
    return;
  }

  // 检查 extract 规则
  for (let i = 0; i < mutateRules.length; i++) {
    if (mutateRules[i].extract) {
      const extract = mutateRules[i].extract;
      if ('' in extract) {
        return 'datasource.transformer.extractrule.nofield';
      }
      for (const key in extract) {
        if ('' in extract[key]) {
          return 'datasource.transformer.extractrule.norule';
        }
      }
    }
  }
}

export function filterEmpty(val: any) {
  if (Object.is(val, undefined) || Object.is(val, '') || Object.is(val, null)) {
    return '';
  }
  if (Object.is(val, 0) || Object.is(val, false) || Object.is(val, true) || typeof val == 'object') {
    return val.toString();
  }
  return val;
}

export const supportTransform = reactive({
  supportSQL: false,
  supportTransform: false,
  supportTopicBody: false,
  is_sparkplugb: false
});

// 确认 transform 类型
export function configureSupportFlags(data: string) {
  supportTransform.supportSQL =
    data == 'avevaHistorian' || data == 'mysql' || data == 'postgres' || data == 'oracle' || data == 'mssql';
  supportTransform.supportTransform =
    data == 'avevaHistorian' ||
    data == 'mysql' ||
    data == 'postgres' ||
    data == 'oracle' ||
    data == 'mssql' ||
    data == 'kafka' ||
    data == 'mqtt' ||
    data == 'mongodb' ||
    data == 'sparkplugb';
  supportTransform.supportTopicBody = data == 'mqtt' || data == 'sparkplugb' || data == 'kafka';
  supportTransform.is_sparkplugb = data == 'sparkplugb'
}

const initialState: TransformerState = {
  csvParser: null,
  transformExtractParseData: null,
  csvTransformerParser: null,
  transformerFilterParseData: null,
  transformerMapCloumns: [],
  transformerParserData: null,
  transformColumnIdentify: [],
  csvTransformerlocalCols: [], //csv无头部时候的自定义列
  splitExpresList: null, //transformer的split
  mappingjoin: '', //mapping时候映射值是join时候的
  definitions: [],
  topParse: null,
  transformResultTable: [],
  createStWithoutDB: 0,
  transformTableHeight: 0,
  transformerfullparams: null,
  transResultName: '',
  historianechodata: null,
  s_model: {},
  limitOffset: 5,
  showResultTb: false,
  resultTbTitle: '',
  activeColumns: [] as string[], // 转换拆分出来的新字段
  resultCurrentPage: 1,
  stbDefaultColumns: [], // transfrom 创建超级表时默认的列
  convertExpresList: null,
  jsonExtractListType: null,
};

// 用一个大的对象包裹起来 方便数据管理和赋值
export const transformerState = reactive<TransformerState>({ ...initialState });

export const resetTransformerState = () => {
  Object.assign(transformerState, { ...initialState });
};

export const defaultColsMap: Record<string, string[]> = {
  mqtt: ['topic', 'qos'],
  kafka: ['topic', 'partition', 'offset'],
  mongodb: ['value']
};

export const hiddenColsMap: Record<string, string[]> = {
  mqtt: ['ts', 'qos', 'topic'],
  kafka: ['ts', 'topic', 'partition', 'offset'],
  mongodb: ['ts']
};
