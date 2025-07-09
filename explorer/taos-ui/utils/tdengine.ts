import { TDengineStringType, TDengineNumberType, DBParameters, VariableTableColumnType, TwoVariableTableColumnType } from 'constants1/tdengine';
/**
 * @description 针对TDengine的restful接口中返回的head和data，返回一个适合table组件的对象
 * @author 阿宾
 * @date 18/07/2024
 * @export
 * @param {string[]} head
 * @param {string[][]} data
 * @returns {*}  {Recordable[]}
 */
export function compHeadAndData(head: string[][], data: string[][]): Recordable[] {
  return data.map(item =>
    Object.fromEntries(
      head.map((a, b) => {
        let value = item[b];
        if (value === null) {
          value = 'null';
        }
        return [a[0], value];
      })
    )
  );
}

/**
 * @description 比较版本号，支持>、<、=操作符，返回true或false，支持多位版本号，如1.0.0、
 * @author 阿宾
 * @date 19/07/2024
 * @export
 * @param {string} currentVersion
 * @param {string} targetVersion
 * @returns {*}
 */
export function compareVersion(currentVersion: string, targetVersion: string): boolean {
  const v1Arr = currentVersion.split('.');
  const compareOperator = targetVersion.match(/^[><=]+/)?.[0] || '>';
  const v2Arr = targetVersion.replace(compareOperator, '').split('.');
  while (v1Arr.length || v2Arr.length) {
    const v1 = Number(v1Arr.shift() || 0);
    const v2 = Number(v2Arr.shift() || 0);
    if (v1 > v2) return compareOperator.includes('>');
    if (v1 < v2) return compareOperator.includes('<');
    if (v1 == v2 && v1Arr.length == 0 && v2Arr.length == 0) return compareOperator.includes('=');
  }
  return false;
}

// 转义sql中的特殊字符自动添加反斜杠
export function escapeSpecialChar(str: string): string {
  return str.replace(/\\/g, '\\\\').replace(/'/g, "\\'").replace(/"/g, '\\"');
}

// 获取数据库字段类型
export function getFieldType(type: string): string | '' | 'NUMBER' | 'STRING' {
  if (!type) return '';
  type = type.replace(/\(\d+\)/, '');
  if (TDengineStringType.includes(type)) return 'STRING';
  if (TDengineNumberType.includes(type)) return 'NUMBER';
  return type;
}

export function getTypeAndLength(type: string) {
  if (!type) return '';
  const types = type.match(/([^\d]+)+\((\d+\))/);
  let typeStr, length;
  if (!types || types.length === 0) {
    typeStr = type;
  } else if (types.length > 1) {
    typeStr = types[1];
    if (types.length === 3) {
      length = parseInt(types[2]);
    }
  }
  return {
    type: typeStr,
    length: length ? length : 0
  };
}

// 根据 td 版本获取数据库参数列表
export function getDbParamsByTdVersion(
  version: string,
  resultType: 'object' | 'array' = 'object'
): Recordable | Recordable[] {
  const params = DBParameters.filter(item => !version || !item?.version || compareVersion(version, item.version));
  if (resultType == 'array') return params;
  return Object.fromEntries(params.map(item => [item.name, item.defaultValue]));
}

// 处理字段名称，并且添加反引号
export function addStrBackquote(name: string) {
  if (/`[^`]+`/.test(name)) return name;
  return '`' + name + '`';
}

// 移除字符串中首尾的反引号
export function rmStrBackquote(name: string) {
  if (!/`[^`]+`/.test(name)) return name;
  return name.slice(1, -1);
}

export interface ComposeTypeParameter {
  type: string;
  length: number;
  length2: number;
}
export function composeType(data: ComposeTypeParameter) {
  const { type, length, length2 } = data;
  if (VariableTableColumnType.includes(type)) {
    return `${type}(${length})`;
  }
  if (TwoVariableTableColumnType.includes(type)) {
    return `${type}(${length},${length2})`
  }
  return type;
}

export function processStringTagValue(dataType: string, value: string) {
  if (VariableTableColumnType.some(item => dataType.startsWith(item)) || dataType == 'JSON') {
    return `'${value}'`;
  } else {
    return value;
  }
}
