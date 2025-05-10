import dayJs from './date';
import type { ManipulateType } from 'dayjs';
import { TDengineSqlKeywrods, AggregationFn } from 'constants1/index';

const toString = Object.prototype.toString;

/**
 * @description 检查权限是否存在于权限列表中，支持多个权限匹配，支持匹配全部或者匹配一个即可通过检查
 * @author 阿宾
 * @date 17/07/2024
 * @export
 * @param {(string | string[])} privilege
 * @param {string[]} privilegeList
 * @param {boolean} [matchAll=false]
 * @returns {*}
 */
export function checkPrivilege(privilege: string | string[], privilegeList: string[], matchAll = true) {
  if (isArray(privilege)) {
    if (matchAll) {
      return privilege.every(item => privilegeList.includes(item));
    } else {
      return privilege.some(item => privilegeList.includes(item));
    }
  }
  return privilegeList.includes(privilege);
}

export function is(val: unknown, type: string) {
  return toString.call(val) === `[object ${type}]`;
}
/**
 * @param {string} path
 * @returns {Boolean}
 */
export function isExternal(path: string) {
  return /^(https?:|mailto:|tel:)/.test(path);
}

/**
 * @param {string} str
 * @returns {Boolean}
 */
export function isString(str: unknown): str is string {
  if (typeof str === 'string' || str instanceof String) {
    return true;
  }
  return false;
}

/**
 * @param {Array} arg
 * @returns {Boolean}
 */
export function isArray(arg: unknown): arg is unknown[] {
  if (typeof Array.isArray === 'undefined') {
    return is(arg, 'Array');
  }
  return Array.isArray(arg);
}

/**
 * @param {Object} arg
 * @returns {Boolean}
 */
export function isNumber(arg: unknown) {
  return arg !== null && is(arg, 'Number');
}

/**
 * @param {Object} arg
 * @returns {Boolean}
 */
export function isObject(arg: unknown): arg is Record<string, unknown> {
  return arg !== null && is(arg, 'Object');
}

/**
 * @param {Object} arg
 * @returns {Boolean}
 */
export function isEmpty(arg: unknown) {
  if (isArray(arg) || isString(arg)) {
    return arg.length === 0;
  }

  if (arg instanceof Map || arg instanceof Set) {
    return arg.size === 0;
  }

  if (isObject(arg)) {
    return Object.keys(arg).length === 0;
  }
  return false;
}

/**
 * @param {Object} arg
 * @returns {Boolean}
 */
export function isFunction(arg: unknown) {
  return typeof arg === 'function';
}
/**
 * @param {Object} arg
 * @returns {Boolean}
 */
export function isBoolean(arg: unknown) {
  return is(arg, 'Boolean');
}

/**
 * @param {Object} arg
 * @returns {Boolean}
 */
export function isRegExp(arg: unknown) {
  return is(arg, 'RegExp');
}

/**
 * @param {Object} arg
 * @returns {Boolean}
 */
export function isPromise(arg: unknown) {
  return is(arg, 'Promise') && isObject(arg) && isFunction(arg.then) && isFunction(arg.catch);
}

/**
 * @param {Object} arg
 * @returns {Boolean}
 */
export function isIterable(arg: unknown) {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return arg !== null && typeof arg === 'object' && typeof (arg as any)[Symbol.iterator] === 'function';
}

/**
 * @param {string} str
 * @returns {Boolean}
 */
export function validUsername(str: string) {
  const valid_map = ['admin', 'editor'];
  return valid_map.indexOf(str.trim()) >= 0;
}
/**
 * @param {string} str
 * @returns {Boolean}
 */
export function validPhone(str: unknown) {
  let pass;
  const val = ('' + str).trim();
  // if ( !/^(0|86|17951)?(13[0-9]|15[0-9]|17[0-9]|18[0-9]|16[0-9]|14[0-9])[0-9]{8}$/i.test( val ) ) {
  if (!/^(0|86|17951)?(13[0-9]|14[0-9]|15[0-9]|16[0-9]|17[0-9]|18[0-9])[0-9]{8}$/i.test(val)) {
    pass = false;
  } else {
    pass = true;
  }
  return pass;
}

/**
 * @param {string} url
 * @returns {Boolean}
 */
export function validURL(url: string) {
  const reg =
    /^(https?|ftp):\/\/([a-zA-Z0-9.-]+(:[a-zA-Z0-9.&%$-]+)*@)*((25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9][0-9]?)(\.(25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])){3}|([a-zA-Z0-9-]+\.)*[a-zA-Z0-9-]+\.(com|edu|gov|int|mil|net|org|biz|arpa|info|name|pro|aero|coop|museum|[a-zA-Z]{2}))(:[0-9]+)*(\/($|[a-zA-Z0-9.,?'\\+&%$#=~_-]+))*$/;
  return reg.test(url);
}

/**
 * @param {string} str
 * @returns {Boolean}
 */
export function validLowerCase(str: string) {
  const reg = /^[a-z]+$/;
  return reg.test(str);
}

/**
 * @param {string} str
 * @returns {Boolean}
 */
export function validUpperCase(str: string) {
  const reg = /^[A-Z]+$/;
  return reg.test(str);
}

/**
 * @param {string} str
 * @returns {Boolean}
 */
export function validAlphabets(str: string) {
  const reg = /^[A-Za-z]+$/;
  return reg.test(str);
}

/**
 * @param {string} email
 * @returns {Boolean}
 */
export function validEmail(email: string) {
  const reg = /^\w+([-+.]\w+)*@\w+(-\w+)*(\.\w+){0,2}([-.][a-zA-Z]+)$/g;
  return reg.test(email);
}
export function validPassword(password: string) {
  return /^(?=.*[a-zA-Z])(?=.*[0-9])(?=.*[._~!@#$^&*])[A-Za-z0-9._~!@#$^&*]{8,20}$/.test(password);
}
/**
 * @param {string} ID
 * @returns {Boolean}
 */
export function validID(str: string) {
  const reg =
    /^[1-9]\d{7}((0\d)|(1[0-2]))(([0|1|2]\d)|3[0-1])\d{3}$|^[1-9]\d{5}[1-9]\d{3}((0\d)|(1[0-2]))(([0|1|2]\d)|3[0-1])\d{3}([0-9]|X)$/;
  return reg.test(str);
}

export function isNull(arg: unknown) {
  return is(arg, 'Null');
}

export function isUnDef(arg: unknown) {
  return is(arg, 'Undefined');
}

export function isNullAndUnDef(arg: unknown) {
  return isNull(arg) && isUnDef(arg);
}

// 校验TDengine版本号目前只支持到x.x.x.x的位数
export function validTDengineImageVersion(version: string) {
  return /^(\d+\.){3}\d+$/.test(version);
}

/**
 * @description 判断是否为 windows 系统
 * @author 阿宾
 * @date 12/07/2024
 * @export
 * @returns {*}
 */
export function isWindows() {
  return navigator.platform.indexOf('Win') > -1;
}

/**
 * @description 名称检测，不能空格下划线、数字开头，不能有特殊字符，不能空格结尾
 * @author YaBin
 * @date 12/07/2024
 * @export
 * @param {string} name
 * @returns {*}
 */
export function validName(name: string) {
  return /^[a-zA-Z\u4e00-\u9fa5][-\sa-zA-Z0-9_\u4e00-\u9fa5]*$/.test(name);
}

/**
 * @description 验证银行账号(含公司账号和个人账号)
 * @author 阿宾
 * @date 09/08/2024
 * @export
 * @param {string} str
 * @returns {*}
 */
export function validBankAccount(str: string) {
  return /^([1-9]{1})\d{11,19}$/.test(str);
}

/**
 * @description 判断日期是否在指定日期之前
 * @author 阿宾
 * @date 08/08/2024
 * @export
 * @param {DateType} date
 * @param {DateType} [dateToCompare=Date.now()]
 * @param {ManipulateType} [unit]
 * @returns {*}  {boolean}
 */
export function isBefore(date: DateType, dateToCompare: DateType = Date.now(), unit?: ManipulateType): boolean {
  return dayJs(date).isBefore(dayJs(dateToCompare), unit);
}

/**
 * @description 判断日期是否在指定日期之后
 * @export
 * @param {DateType} date
 * @param {DateType} [dateToCompare=Date.now()]
 * @param {ManipulateType} [unit]
 * @returns {*}  {boolean}
 */
export function isAfter(date: DateType, dateToCompare: DateType = new Date(), unit?: ManipulateType): boolean {
  return dayJs(date).isAfter(dayJs(dateToCompare), unit);
}

/**
 * @description 判断日期是否相等
 * @export
 * @param {DateType} date
 * @param {DateType} [dateToCompare=Date.now()]
 * @param {ManipulateType} [unit]
 * @returns {*}  {boolean}
 */
export function isEqual(date: DateType, dateToCompare: DateType = Date.now(), unit?: ManipulateType): boolean {
  return dayJs(date).isSame(dayJs(dateToCompare), unit);
}

/**
 * @description 判断日期是否在指定日期之前或相等
 * @export
 * @param {DateType} date
 * @param {DateType} [dateToCompare=Date.now()]
 * @param {ManipulateType} [unit]
 * @returns {*}  {boolean}
 */
export function isSameOrBefore(date: DateType, dateToCompare: DateType = Date.now(), unit?: ManipulateType): boolean {
  return dayJs(date).isSameOrBefore(dayJs(dateToCompare), unit);
}

/**
 * @description 判断日期是否在指定日期之后或相等
 * @export
 * @param {DateType} date
 * @param {DateType} [dateToCompare=Date.now()]
 * @param {ManipulateType} [unit]
 * @returns {*}  {boolean}
 */
export function isSameOrAfter(date: DateType, dateToCompare: DateType = Date.now(), unit?: ManipulateType): boolean {
  return dayJs(date).isSameOrAfter(dayJs(dateToCompare), unit);
}

/**
 * @description 判断日期是否在指定日期之间
 * @export
 * @param {DateType} date
 * @param {DateType} start
 * @param {DateType} end
 * @param {ManipulateType} [unit]
 * @returns {*}  {boolean}
 */
export function isBetween(date: DateType, start: DateType, end: DateType, unit?: ManipulateType): boolean {
  return dayJs(date).isBetween(dayJs(start), dayJs(end), unit);
}

/**
 * @description 验证税号
 * @author 阿宾
 * @date 09/08/2024
 * @export
 * @param {string} str
 * @returns {*}
 */
export function validInvoiceNumber(str: string) {
  return /^[^_IOZSVa-z\W]{2}\d{6}[^_IOZSVa-z\W]{10}$/.test(str);
}

/**
 * @description 校验 dsn 是否合法
 * @author 阿宾
 * @date 22/08/2024
 * @export
 * @param {string} dsn
 * @returns {*}
 */
export function validDsn(dsn: string) {
  return /^taos\+ws(s)?/.test(dsn);
}

/**
 * @description 判断是否为IP地址或者IP地址URL，支持http和https协议，支持端口号，支持IPV4和IPV6地址
 * @author 阿宾
 * @date 22/08/2024
 * @export
 * @param {string} url
 * @returns {*}
 */
export function isIPUrl(url: string) {
  return /^((http|https):\/\/)?(\d{1,3}\.){3}\d{1,3}(:\d{1,5})?\/?/.test(url);
}

/**
 * @description 判断是否为IP地址，支持IPV4和IPV6地址判断
 * @author 阿宾
 * @date 22/08/2024
 * @export
 * @param {string} str
 * @returns {*}
 */
export function isIP(str: string) {
  return isIPV4(str) || isIPV6(str);
}

/**
 * @description 判断是否为IPV4地址
 * @author 阿宾
 * @date 22/08/2024
 * @export
 * @param {string} str
 * @returns {*}
 */
export function isIPV4(str: string) {
  return /([1-9]?\d|1\d{2}|2[0-4]\d|25[0-5])(\.([1-9]?\d|1\d{2}|2[0-4]\d|25[0-5])){3}$/.test(str);
}

/**
 * @description 判断是否为IPV6地址
 * @author 阿宾
 * @date 22/08/2024
 * @export
 * @param {string} str
 * @returns {*}
 */
export function isIPV6(str: string) {
  return /^([a-fA-F0-9]{1,4}:){7}[a-fA-F0-9]{1,4}$/.test(str);
}

/**
 * @description 判断对象中是否包含某个属性
 * @author 阿宾
 * @date 26/08/2024
 * @export
 * @param {object} obj
 * @param {keyof typeof obj} key
 * @returns {*}  {boolean}
 */
export function hasOwnProperty(obj: Recordable, key: keyof typeof obj): boolean {
  return Object.prototype.hasOwnProperty.call(obj, key);
}

// DURATION 100h、DURATION 10d 等，支持 m（分钟）、h（小时）和 d（天）三个单位。不加时间单位时默认单位为天，如 DURATION 50 表示 50 天
export function validDbDuration(duration: string) {
  return /^\d+[mhd]?$/.test(duration);
}

// 表示数据文件保存的天数，缺省值为 3650，取值范围 [1, 365000]，且必须大于或等于 DURATION 参数值。数据库会自动删除保存时间超过 KEEP 值的数据。KEEP 可以使用加单位的表示形式，如 KEEP 100h、KEEP 10d 等，支持 m（分钟）、h（小时）和 d（天）三个单位。也可以不写单位，如 KEEP 50，此时默认单位为天。企业版支持多级存储功能, 因此, 可以设置多个保存时间（多个以英文逗号分隔，最多 3 个，满足 keep 0 <= keep 1 <= keep 2，如 KEEP 100h,100d,3650d）
export function validDbKeep(keep: string) {
  return /^\d+[mhd]?(,\d+[mhd]?){0,2}$/.test(keep);
}

export function validTDKeywords(str: string) {
  if (/`.*`/.test(str)) return false;
  return TDengineSqlKeywrods.includes(str.toUpperCase());
}

// 合法字符：英文字符、数字和下划线,允许英文字符或下划线开头，不允许以数字开头
export function validTableName(str: string) {
  return /^`?[a-zA-Z_]\w*`?$/.test(str);
}

const AggregationFnList = AggregationFn.map(item => item.label);
// 判断sql是否为select语句
export function validSqlIsSelect(sql: string) {
  if (AggregationFnList.some(fn => new RegExp(`select\\s+${fn}\\(?.*\\)?\\s+from`, 'i').test(sql))) return false;
  if (!/from/i.test(sql)) return false;
  return sql.trim().toLowerCase().startsWith('select');
}

// 校验 host
export function validHost(host: string) {
  return /^[a-zA-Z0-9.-]+$/.test(host);
}
