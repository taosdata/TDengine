import jsonBigint from 'json-big';
import { ElNotification } from 'element-plus';
import { t } from 'locales';
import { fromByteArray, toByteArray } from 'base64-js';

const { parse } = jsonBigint({ storeAsString: true });

// 复制文本到剪贴板
export function copy(text: string, callback = () => ElNotification.success(t('msg.copySuccess'))) {
  const polyfillFn = () => {
    const textarea = document.createElement('textarea');
    document.body.appendChild(textarea);
    // 隐藏此输入框
    textarea.style.position = 'fixed';
    textarea.style.left = '-999px';
    textarea.style.top = '10px';
    textarea.setAttribute('readonly', 'readonly');
    // 赋值
    textarea.value = text;
    // 选中
    textarea.select();
    // 复制
    document.execCommand('copy', true);
    // 移除输入框
    document.body.removeChild(textarea);
    callback();
  };
  if (navigator && navigator.clipboard) {
    // clipboard api 复制
    navigator.clipboard.writeText(text).then(callback).catch(polyfillFn);
  } else {
    polyfillFn();
  }
}

/**
 *单词首字母转大写
 * @export
 * @param {string} str
 * @return {*}
 */
export function firstUpperCase(str: string) {
  return str.toLowerCase().replace(/(\s|^)[a-z]/g, L => L.toUpperCase());
}

/**
 * @description 获取剪切板内容
 * @author YaBin
 * @date 04/07/2024
 * @export
 * @param {*} [success=(text: string) => {
 *     console.log(text);
 *   }]
 */
export function getClipboardText(
  success = (text: string) => {
    console.log(text);
  }
) {
  const polyfillFn = () => {
    const textarea = document.createElement('textarea');
    document.body.appendChild(textarea);
    // 隐藏此输入框
    textarea.style.position = 'fixed';
    textarea.style.left = '-999px';
    textarea.style.top = '10px';
    textarea.setAttribute('readonly', 'readonly');
    // 选中
    textarea.select();
    textarea.focus();
    // 复制
    document.execCommand('paste', true);
    // 获取剪切板内容
    const text = textarea.value;
    // 移除输入框
    document.body.removeChild(textarea);
    success(text);
  };
  if (navigator && navigator.clipboard) {
    // clipboard api 复制
    navigator.clipboard.readText().then(success).catch(polyfillFn);
  } else {
    polyfillFn();
  }
}

export const CapacityUnit = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
/**
 * @description B, KB,MB,GB 转换为指定单位
 * @author YaBin
 * @date 04/07/2024
 * @export
 * @param {number} size
 * @param {string} [unit='KB']
 * @param {(string | undefined)} [targetUnit]
 * @param {boolean} [split=false]
 * @returns {*}  {(string| [number, string])}
 */
export function transformSize(
  size: number,
  unit: string,
  targetUnit: string | undefined,
  split: true
): [number, string];
export function transformSize(size: number, unit?: string, targetUnit?: string | undefined, split?: false): string;
export function transformSize(
  size: number,
  unit = 'KB',
  targetUnit?: string | undefined,
  split = false
): string | [number, string] {
  let index = CapacityUnit.indexOf(unit);
  let targetIndex = CapacityUnit.length - 1;
  if (targetUnit) {
    targetIndex = CapacityUnit.indexOf(targetUnit);
  }
  if (index === -1) {
    throw new Error('unit error');
  }
  let result = size;
  while (index < targetIndex && (result >= 1024 || targetUnit)) {
    result = result / 1024;
    index++;
  }
  result = handleFloat(result, result < 0.01 ? 4 : 2);
  const resultUnit = targetUnit || CapacityUnit[index];
  if (split) {
    return [result, resultUnit];
  }
  return result + ' ' + resultUnit;
}

/**
 * @description 转换容量占比
 * @author YaBin
 * @date 04/07/2024
 * @export
 * @param {(string | number)} used
 * @param {(string | number)} total
 * @param {string} [unit='MB']
 * @returns {*}  {string}
 */
export function transformCapacityPercent(used: string | number, total: string | number, unit = 'MB'): string {
  if (!used || !total) return (used ?? 0) + '/' + (total ?? 0) + ' ' + unit;
  used = Number(used);
  total = Number(total);
  let index = CapacityUnit.indexOf(unit);
  if (isNaN(used) || isNaN(total)) return used + '/' + total + ' ' + unit;
  while (used >= 1024 && index < CapacityUnit.length) {
    used = used / 1024;
    total = total / 1024;
    index++;
  }
  [used, total] = [used, total].map((item: number) => {
    if (String(item).includes('.')) {
      item = handleFloat(item, item < 0.01 ? 3 : 2);
    }
    return item;
  });
  return used + '/' + total + ' ' + CapacityUnit[index];
}

/**
 * @description 处理浮点数
 * @author YaBin
 * @date 04/07/2024
 * @export
 * @param {(number | string)} num
 * @param {number} [fixed=2]
 * @returns {*}  {number}
 */
export function handleFloat(num: number | string, fixed = 2): number {
  num = Number(num);
  if (isNaN(num)) return num;
  return Number(num.toFixed(fixed));
}

/**
 * @description 对html文本进行转义
 * @author YaBin
 * @date 04/07/2024
 * @export
 * @param {string} str
 * @returns {*}  {string}
 */
export function escapeHtml(str: string): string {
  return str.replace(/[&<>"']/g, function (match) {
    switch (match) {
      case '&':
        return '&amp;';
      case '<':
        return '&lt;';
      case '>':
        return '&gt;';
      case '"':
        return '&quot;';
      case "'":
        return '&#39;';
      default:
        return match;
    }
  });
}

/**
 * @description 获取 html 中的文本节点内容
 * @author 阿宾
 * @date 08/08/2024
 * @export
 * @param {string} html
 * @returns {*}
 */
export function htmlToText(html: string) {
  return html
    .replace(/<style[^>]*>[\s\S]*?<\/style>/gi, '')
    .replace(/<script[^>]*>[\s\S]*?<\/script>/gi, '')
    .replace(/<script[^>]*>[\s\S]*?<\/script>/gi, '')
    .replace(/<head[^>]*>[\s\S]*?<\/head>/gi, '')
    .replace(/<[^>]+>/g, '');
}

/**
 * @description 处理在线时间的展示格式，如 1d2h3m4s，返回 1d2h 或 1d2h3m 或 1d2h3m4s，不会返回 1d2h3m4s5s 这种情况
 * @author YaBin
 * @date 04/07/2024
 * @export
 * @param {(number | null)} time
 * @param {string} [unit='s']
 * @returns {*}  {string}
 */
export function transformUpTime(time: number | null, unit = 's'): string {
  if (!time) return '0';
  // 目前返回的是秒
  if (time < 1) {
    time = 1;
  }
  time = parseInt(time);
  const timeUnit = ['s', 'min', 'h', 'd', 'm'];
  const timeDur = [60, 60, 24, 30];
  let index = timeUnit.indexOf(unit);
  const fn = (time: number): string => {
    if (time >= timeDur[index]) {
      // 当到达月的时候不再拼接
      if (index == timeDur.length) {
        return time + timeUnit[index];
      }
      const re = time % timeDur[index] ? (time % timeDur[index]) + timeUnit[index] : '';
      return fn(Math.floor(time / timeDur[index++])) + re;
    } else {
      return time + timeUnit[index];
    }
  };
  return fn(time).match(/(\d+[^\d]+){1,2}/g)?.[0] ?? '';
}

/**
 * @description 生成随机 uuid
 * @author YaBin
 * @date 04/07/2024
 * @export
 * @returns {*}
 */
export function uuid() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

/**
 * @description json to object
 * @author 阿宾
 * @date 16/07/2024
 * @export
 * @param {string} data
 * @returns {*}
 */
export function jsonToObj(data: string) {
  if (typeof data != 'string') return {};
  let result;
  try {
    result = parse(data);
  } catch {
    result = {};
  }
  return typeof result == 'object' ? result : {};
}

/**
 * @description 将blob转换为对象
 * @author 阿宾
 * @date 17/07/2024
 * @export
 * @param {*} blob
 * @returns {*}
 */
export function blobToObject(blob: Blob): Promise<any> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onloadend = () => {
      try {
        const result = JSON.parse(reader.result as string);
        resolve(result);
      } catch (error) {
        reject(null);
      }
    };
    reader.onerror = reject;
    reader.readAsText(blob);
  });
}

/**
 * @description 打开新窗口
 * @author 阿宾
 * @date 17/07/2024
 * @export
 * @param {string} url
 * @param {string} [target='_blank']
 * @returns {*}
 */
export function openNewWindow(url: string) {
  const win = window.open(url, '_blank');
  if (win) return;
  const a = document.createElement('a');
  a.target = '_blank';
  a.href = url;
  a.style.display = 'none';
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
}

/**
 * @description 移除字符串中的特殊字符
 * @author 阿宾
 * @date 09/08/2024
 * @export
 * @param {string} str
 * @returns {*}
 */
export function removeSpecialChar(str: string) {
  return str.replace(/[^\w\s]/gi, '');
}

const TimeUnit = ['s', 'min', 'h', 'd', 'm'];
/**
 * @description 处理在线时间的展示
 * @author 阿宾
 * @date 14/08/2024
 * @export
 * @param {(number | null)} time
 * @returns {*}
 */
export function processUptime(time: number | null) {
  if (!time) return '0';
  // 目前返回的是秒
  if (time < 1) {
    time = 1;
  }
  time = parseInt(time);
  const dur = [60, 60, 24, 30];
  let index = 0;
  const fn = (time: number): string => {
    if (time >= dur[index]) {
      // 当到达月的时候不再拼接
      if (index == dur.length) {
        return time + TimeUnit[index];
      }
      const re = time % dur[index] ? (time % dur[index]) + TimeUnit[index] : '';
      return fn(Math.floor(time / dur[index++])) + re;
    } else {
      return time + TimeUnit[index];
    }
  };
  return fn(time).match(/(\d+[^\d]+){1,2}/g)?.[0] ?? '';
}

export function requestInterval(fn: AnyFunction, delay: number, immediate = true, once = false) {
  let start = immediate ? Date.now() - delay : Date.now();
  let pause = false;
  const loop = () => {
    if (pause) return;
    if (Date.now() - start >= delay) {
      fn();
      start = Date.now();
      if (!once) requestAnimationFrame(loop);
    } else {
      requestAnimationFrame(loop);
    }
  };
  requestAnimationFrame(loop);
  return () => (pause = true);
}

// 根据图表轴的数据判断轴的类型
export function getAxisType(data: string | number): 'category' | 'value' | 'time' {
  if (Number(data) === 0) return 'value';
  if (!data) return 'category';
  if (!isNaN(Number(data))) return 'value';
  if (new Date(data).toString() != 'Invalid Date') return 'time';
  return 'category';
}

// 获取鼠标在元素内的位置
export function getMousePosition(e: MouseEvent) {
  const target = e.target as HTMLElement;
  return {
    x: e.clientX - target.offsetLeft,
    y: e.clientY - target.offsetTop
  };
}

/**
 * @description base64 编码解码工具
 */

export const base64Utils = {
  encode: (str: string): string => {
    const bytes = new TextEncoder().encode(str);
    return fromByteArray(bytes);
  },

  decode: (base64: string): string => {
    const cleanBase64 = base64.replace(/[^A-Za-z0-9+/=]/g, '');
    const bytes = toByteArray(cleanBase64);
    return new TextDecoder().decode(bytes);
  }
};
