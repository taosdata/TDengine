import moment from 'moment';
import momentTimezone from 'moment-timezone';
import { ElMessage } from 'element-plus';
import CryptoJS from 'crypto-js';
import i18n from '@/lang/index';
// import _, { pad } from "lodash";
import * as clipboard from 'clipboard-polyfill';

export function deepClone(source) {
  if (!source && typeof source !== 'object') {
    throw new Error('error arguments', 'deepClone');
  }
  const targetObj = source?.constructor === Array ? [] : {};
  Object.keys(source).forEach(keys => {
    if (source[keys] && typeof source[keys] === 'object') {
      targetObj[keys] = deepClone(source[keys]);
    } else {
      targetObj[keys] = source[keys];
    }
  });
  return targetObj;
}

export function parseTime(time, cFormat) {
  return moment(time).format(cFormat);
}

/**
 * 针对TDengine的restful接口中返回的head和data，返回一个适合table组件的对象
 * @param head 表头数组，每项为 [字段名, ...]
 * @param data 数据数组，每项为字段值数组
 * @returns 对象数组，键为字段名，值为对应数据
 */
export function compHeadAndData(head: Array<[string, ...any[]]>, data: any[][]): Record<string, any>[] {
  if (!Array.isArray(head) || !Array.isArray(data)) return [];
  const keys = head.map(h => h[0]);
  return data.map(row => {
    const obj: Record<string, any> = {};
    keys.forEach((key, idx) => {
      obj[key] = row[idx] ?? '';
    });
    return obj;
  });
}
function handlerData(data) {
  return data
    .map(field => {
      // 如果字段中包含逗号或双引号，则用双引号包裹，并且内部的双引号需要转义
      if (field.includes(',') || field.includes('"')) {
        return `"${field.replace(/"/g, '""')}"`;
      } else {
        return field;
      }
    })
    .join(',');
}
/**
 * 将table数据转成csv数据
 * @param {Array<Record<string, any>>} data 表格数据
 * @param {Array<string>} head 表头数据
 * @returns
 */
export function convertToCsvData(data, head) {
  const csvHeader = handlerData(head);
  const csvRows = data.map(row => {
    return handlerData(row);
  });
  return csvHeader + '\n' + csvRows.join('\n');
}

// 下划线转换驼峰
export function toHump(name) {
  return name.replace(/_(\w)/g, function (all, letter) {
    return letter.toUpperCase();
  });
}
// 驼峰转换下划线
export function toLine(name) {
  return name.replace(/([A-Z])/g, '_$1').toLowerCase();
}

//转换对象下划线到驼峰
export function objToHump(target) {
  if (typeof target != 'object') return {};
  const obj = {};
  Object.keys(target).forEach(item => {
    obj[toHump(item)] = target[item];
  });
  return obj;
}

//转换对象驼峰到下划线
export function objToLine(target) {
  if (typeof target != 'object') return {};
  const obj = {};
  Object.keys(target).forEach(item => {
    obj[toLine(item)] = target[item];
  });
  return obj;
}

// json to object
export function jsonToObj(data) {
  if (typeof data != 'string') return {};
  let result;
  try {
    result = JSON.parse(data);
  } catch {
    result = {};
  }
  return typeof result == 'object' ? result : {};
}

export function download(url, filename) {
  // 创建隐藏的可下载链接
  const eleLink = document.createElement('a');
  eleLink.download = filename;
  eleLink.style = {
    display: 'none',
    position: 'fixed'
  };
  eleLink.href = url;
  // 触发点击
  document.body.appendChild(eleLink);
  eleLink.click();
  // 然后移除
  document.body.removeChild(eleLink);
}

function handler(text) {
  clipboard.writeText(text).then(
    () => {
      console.log('success!', '写入成功,text');
    },
    () => {
      console.log('error!');
    }
  );
}
export function copy(text, success = () => ElMessage.success(i18n.global.t('copySucc'))) {
  const copyButton = document.getElementById('copyButton');
  if (copyButton) {
    document.body.removeChild(copyButton);
  }
  const button = document.body.appendChild(document.createElement('button'));
  button.textContent = text;
  button.id = 'copyButton';
  button.addEventListener('click', handler(text));
  button.style.display = 'none';

  success();
}

// 生成随机id
export function guid() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
    const r = (Math.random() * 16) | 0,
      v = c == 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

export function HtmlToText(html) {
  return html
    .replace(/<(style|script|iframe)[^>]*?>[\s\S]+?<\/\1\s*>/gi, '')
    .replace(/<[^>]+?>/g, '')
    .replace(/\s+/g, ' ')
    .replace(/ /g, ' ')
    .replace(/>/g, ' ');
}

export function OpenNewTab(url: string) {
  const win = window.open(url, '_blank');
  if (win) return;

  const a = window.document.createElement('a');
  a.target = '_blank';
  a.href = url;
  const e = new MouseEvent('click');
  e.stopPropagation();
  a.dispatchEvent(e);
}

//删除cookie某一项目
export function deleteCookieItem() {
  const cookieItems = document.cookie.split(';');
  for (let i = 0; i < cookieItems.length; i++) {
    let item = cookieItems[i];
    while (item.charAt(0) === ' ') {
      item = item.substring(1);
    }
    if (item.indexOf('TDengine-Token=') === 0) {
      document.cookie = encodeURIComponent(item.split('=')[0]) + '=; expires=Thu, 01 Jan 1970 00:00:00 UTC; path=/;';
      break;
    }
  }
}

//加密
export function encrypt(data) {
  const encryptedData = CryptoJS.AES.encrypt(
    data,
    // spellchecker:off
    `-----BEGIN PUBLIC KEY-----
  MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQC//nB6rRTnxCU2bMBGatp1N1Q0
  kuSEZl3Ot2EQMlNwINYTm7izxjTyA1pgmBmotAXVZuZNviJNUZUMBn73bIjso1l2
  qhwe/FcewPjP2ubbdf89yWPnen/wRGo+Q0QRmt1q7eDeVTJMC4LVdetuv6QABnUJ
  +siG1ILDsJ2BsYMBMwIDAQAB
  -----END PUBLIC KEY-----`
    // spellchecker:on
  ).toString(); // 使用AES算法加密数据
  return encryptedData;
}
//解密
export function decrypt(encryptedData: string) {
  if (!encryptedData) {
    console.warn('encryptedData is empty');
    return '';
  }
  const decryptedMessage = CryptoJS.AES.decrypt(
    encryptedData,
    // spellchecker:off
    `-----BEGIN PUBLIC KEY-----
  MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQC//nB6rRTnxCU2bMBGatp1N1Q0
  kuSEZl3Ot2EQMlNwINYTm7izxjTyA1pgmBmotAXVZuZNviJNUZUMBn73bIjso1l2
  qhwe/FcewPjP2ubbdf89yWPnen/wRGo+Q0QRmt1q7eDeVTJMC4LVdetuv6QABnUJ
  +siG1ILDsJ2BsYMBMwIDAQAB
  -----END PUBLIC KEY-----`
    // spellchecker:on
  ).toString(CryptoJS.enc.Utf8); // 使用AES算法解密数据

  return decryptedMessage;
}

// 获取当前集群DSN
export function getDSN(driver = 'tmq', subject = null) {
  const url = localStorage.getItem('base_url');
  if (url.includes('://')) {
    const parsed_url = new URL(url);
    let host = parsed_url.host;
    let scheme = null;
    if (parsed_url.protocol == 'http:') {
      scheme = '+ws';
    } else if (parsed_url.protocol == 'https:') {
      scheme = '+wss';
    } else {
      driver = '';
      scheme = parsed_url.protocol.replace(':', '');
      host = parsed_url.pathname.split('//')[1];
    }

    const user = localStorage.getItem('username') || '';
    const decrypted = encodeURIComponent(decrypt(localStorage.getItem('pwd')));
    const pass = decrypted || '';
    const subjectStr = subject ? '/' + subject : '';
    return driver + scheme + '://' + user + ':' + pass + '@' + host + subjectStr + parsed_url.search;
  } else {
    const host = url;
    const user = localStorage.getItem('username') || '';
    const decrypted = encodeURIComponent(decrypt(localStorage.getItem('pwd')));
    const pass = decrypted || '';
    const subjectStr = subject ? '/' + subject : '';
    return driver + '://' + user + ':' + pass + '@' + host + subjectStr;
  }
}

// 获取时区
export function getLocalTimezone() {
  return localStorage.getItem('timezone') || moment.tz.guess(true) || 'UTC';
}

/**
 * 根据所选时区和格式转化时间
 * @param value
 * @param format
 * @returns
 */
export function parsinginZone(value, format?: string) {
  if (!value) return value;
  const timezone = getLocalTimezone();
  return momentTimezone(value).tz(timezone).format(format);
}

export function formatTime(time) {
  const timezone = getLocalTimezone();
  const str = moment.tz(timezone).format('Z');
  const time1 = moment(time).format();
  const arr = time1.split('+');
  return arr[0] + str;
}

export function getLocalLang() {
  return (i18n.global.locale as WritableComputedRef<string>).value.includes('zh') ? 'zh' : 'en';
}

// 根据图表轴的数据判断轴的类型
export function getAxisType(data) {
  if (!data) return 'category';
  if (!isNaN(data)) return 'value';
  if (new Date(data).toString() != 'Invalid Date') return 'time';
  return 'category';
}
function pad1(n) {
  return n < 10 ? '0' + n : n;
}
export function getRFC3339Time() {
  const d = new Date();
  return (
    d.getUTCFullYear() +
    '-' +
    pad1(d.getUTCMonth() + 1) +
    '-' +
    pad1(d.getUTCDate()) +
    'T' +
    pad1(d.getUTCHours()) +
    ':' +
    pad1(d.getUTCMinutes()) +
    ':' +
    pad1(d.getUTCSeconds()) +
    'Z'
  );
}

function getAllProperties(obj, deep) {
  const properties = [];

  function traverse(prefix, obj, my_deep) {
    for (const key in obj) {
      if (my_deep < deep && typeof obj[key] === 'object' && !Array.isArray(obj[key])) {
        traverse(`${prefix}["${key}"]`, obj[key], my_deep + 1);
      } else {
        properties.push(`${prefix}["${key}"]`);
      }
    }
  }

  traverse('$', obj, 0);
  return properties;
}

/**
 *
 * @param sampleData
 * @param deep
 * @returns
 */
export function extractAllProperties(sampleData: string, deep: string | number) {
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

/**
 * 获取示例数据字符串列表[] 返回字符串，则为错误信息
 * @param demo_data
 * @param parsed
 * @returns
 */
export function getExampleList(demo_data: string, parsed?: boolean) {
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
        const item_parsed = JSON.parse(obj_list[i]);
        if (parsed) {
          demo_string_arr.push(item_parsed);
        } else {
          demo_string_arr.push(obj_list[i]);
        }
      } catch (err) {
        err.lineNumber = i + 1;
        throw err;
      }
    }
  } else {
    throw 'datasource.transformer.jsontip';
  }
  return demo_string_arr;
}

/**
 * 比较TDengine 版本
 * @param currentVersion
 * @param targetVersion
 * @returns
 */

export function compareVersion(currentVersion: string, targetVersion: string) {
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

export function getPassword(): string {
  return encodeURIComponent(decrypt(localStorage.getItem('pwd') || '')) || '';
}

export function getUser(): string {
  return localStorage.getItem('username') || '';
}

export function getBaseUrl(): string {
  return localStorage.getItem('native_url') || localStorage.getItem('base_url') || '';
}

export function getTDVersion(): string {
  return localStorage.getItem('td_version') || '';
}

export function getClusterID(): string {
  return localStorage.getItem('local_clusterID') || '';
}
