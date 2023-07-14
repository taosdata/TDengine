import { viewFile } from "@/api/gateway/support";
import axios from "axios";
import { marked } from "marked";
import moment from "moment";
import momentTimezone from 'moment-timezone';
import { Message } from "element-ui";
import { $bus } from "@/const";
import CryptoJS from "crypto-js";
import i18n from "@/lang";
let path = require("path");
export function debounce(func, wait, immediate) {
  let timeout, args, context, timestamp, result;

  const later = function () {
    //距上一次的时间间隔
    const last = Date.now() - timestamp;

    //上次被包装函数被调用时间间隔 last 小于设定的时间间隔 wait
    if (last < wait && last > 0) {
      timeout = setTimeout(later, wait - last);
    } else {
      timeout = null;
      // 如果设定了immediate === true，说明开始已经调用过了此处无需调用
      if (!immediate) {
        result = func.apply(context, args);
        if (!timeout) {
          context = args = null;
        }
      }
    }
  };

  return function (...rest) {
    args = rest;
    context = this;
    timestamp = Date.now();
    const callNow = immediate && !timeout;
    // 如果延时不存在，重新设定延时
    if (!timeout) timeout = setTimeout(later, wait);
    if (callNow) {
      result = func.apply(context, args);
      context = args = null;
    }
    return result;
  };
}

export function deepClone(source) {
  if (!source && typeof source !== "object") {
    throw new Error("error arguments", "deepClone");
  }
  const targetObj = source.constructor === Array ? [] : {};
  Object.keys(source).forEach(keys => {
    if (source[keys] && typeof source[keys] === "object") {
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

// 针对TDengine的restful接口中返回的head和data，返回一个适合table组件的对象
export function compHeadAndData(head, data) {
  return data.map(item => Object.fromEntries(head.map((a, b) => [a[0], item[b] || ""])));
}
export function customizeTimeout(callback, time, once = 1) {
  let timer = null;
  let startTime = Date.now();
  let currentFrequency = 0;
  let loop = () => {
    currentFrequency++;
    let endTime = Date.now();
    if (endTime - startTime >= time) {
      startTime = endTime = Date.now();
      callback(timer);
      if (currentFrequency < once) loop();
    }
    timer = window.requestAnimationFrame(loop);
  };
  loop();
  return timer;
}

// 下划线转换驼峰
export function toHump(name) {
  return name.replace(/_(\w)/g, function (all, letter) {
    return letter.toUpperCase();
  });
}
// 驼峰转换下划线
export function toLine(name) {
  return name.replace(/([A-Z])/g, "_$1").toLowerCase();
}

//转换对象下划线到驼峰
export function objToHump(target) {
  if (typeof target != "object") return {};
  let obj = {};
  Object.keys(target).forEach(item => {
    obj[toHump(item)] = target[item];
  });
  return obj;
}

//转换对象驼峰到下划线
export function objToLine(target) {
  if (typeof target != "object") return {};
  let obj = {};
  Object.keys(target).forEach(item => {
    obj[toLine(item)] = target[item];
  });
  return obj;
}

// json to object
export function jsonToObj(data) {
  if (typeof data != "string") return {};
  let result;
  try {
    result = JSON.parse(data);
  } catch {
    result = {};
  }
  return typeof result == "object" ? result : {};
}

let fileCache = new Map();
export async function getUrl(id) {
  let cache = fileCache.get(id);
  if (cache) return cache;
  let data = await viewFile(id).catch(() => false);
  if (!data) return {};
  let url;
  try {
    url = URL.createObjectURL(data);
  } catch (error) {
    console.log(error);
  }
  fileCache.set(id, url);
  return url;
}

export function download(url, filename) {
  // 创建隐藏的可下载链接
  var eleLink = document.createElement("a");
  eleLink.download = filename;
  eleLink.style = {
    display: "none",
    positon: "fixed",
  };
  eleLink.href = url;
  // 触发点击
  document.body.appendChild(eleLink);
  eleLink.click();
  // 然后移除
  document.body.removeChild(eleLink);
}

export function copy(text, success = () => Message.success(i18n.t("copySucc"))) {
  let polyfillFn = () => {
    var textarea = document.createElement("textarea");
    document.body.appendChild(textarea);
    // 隐藏此输入框
    textarea.style.position = "fixed";
    textarea.style.left = "-999px";
    textarea.style.top = "10px";
    textarea.setAttribute("readonly", "readonly");
    // 赋值
    textarea.value = text;
    // 选中
    textarea.select();
    // 复制
    document.execCommand("copy", true);
    // 移除输入框
    document.body.removeChild(textarea);
    success();
  };
  if (window.copy) {
    window.copy(text);
    return success();
  }
  if (navigator && navigator.clipboard) {
    // clipboard api 复制
    navigator.clipboard.writeText(text).then(success).catch(polyfillFn);
  } else {
    polyfillFn();
  }
}

// 处理md并且赋值变量生成html展示
export function getDocsContent(url, params) {
  return axios
    .get(url)
    .then(res => {
      // 去掉不属于md的语法
      res.data = res.data.replace(/---[^-]*---/gm, "");
      let result = marked.parse(
        res.data.replace(/```bash\n(((((?!```).)?)+([<.*>]))\n?)+```/gm, str => {
          return str.replace(/<(.*)>/gm, (_, key) => {
            return params[key] || "";
          });
        })
      );
      // 处理图片地址
      let imgRootUrl = url.split("/").slice(0, -1).join("/");
      result = result.replace(/<img [^>]*src=['"]([^'"]+)[^>]*>/gm, (str, u) => {
        return str.replace(u, path.join(imgRootUrl, u));
      });
      return result;
    })
    .catch(() => "");
}

/**
 * 自动根据context内容注册vue组件
 * useage:
 * const regComs from '@/utils/reg';
 * const allComs = require.context('./', true, /\.vue$/);
 * regComs(allComs);
 */
// export default function importFile(vue, url = "./", suffix = ".vue") {
//   const list = [];
//   const context = require.context(url, true, new RegExp(`/\\${suffix}$/`));
//   if (!context || !context.keys) {
//     return list;
//   }
//   context.keys().forEach(file => {
//     const config = context(file);
//     const fileName = file.replace(/^.\//, "").replace(/\.vue$/, "");
//     vue.component(fileName, config.default || config);
//     list.push(fileName);
//   });
//   return list;
// }

// 生成随机id
export function guid() {
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, function (c) {
    var r = (Math.random() * 16) | 0,
      v = c == "x" ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

export function HtmlToText(html) {
  return html
    .replace(/<(style|script|iframe)[^>]*?>[\s\S]+?<\/\1\s*>/gi, "")
    .replace(/<[^>]+?>/g, "")
    .replace(/\s+/g, " ")
    .replace(/ /g, " ")
    .replace(/>/g, " ");
}


export function BusOnAndAutoOff(name, fn) {
  $bus.on(name, fn);
  this.$once("hook:beforeDestroy", () => {
    $bus.off(name, fn);
  });
}

export function OpenNewTab(url) {
  const win = window.open(url, "_blank");
  if (win) return;

  var a = window.document.createElement("a");
  a.target = "_blank";
  a.href = url;
  const e = new MouseEvent("click");
  e.stopPropagation();
  a.dispatchEvent(e);
}

 //删除cookie某一项目
 export function deleteCookieItem() {
  var cookieItems = document.cookie.split(";");
  for (var i = 0; i < cookieItems.length; i++) {
    var item = cookieItems[i];
    while (item.charAt(0) === " ") {
      item = item.substring(1);
    }
    if (item.indexOf("TDengine-Token=") === 0) {
      document.cookie =
        encodeURIComponent(item.split("=")[0]) +
        "=; expires=Thu, 01 Jan 1970 00:00:00 UTC; path=/;";
      break;
    }
  }
}

 //加密
export function encrypt(data) {
  let encryptedData = CryptoJS.AES.encrypt(data, 'pwd').toString(); // 使用AES算法加密数据
  return encryptedData;
}
//解密
export function decrypt(encryptedData) {
  let decryptedMessage = CryptoJS.AES.decrypt(encryptedData,'pwd').toString(CryptoJS.enc.Utf8); // 使用AES算法解密数据

  return decryptedMessage;
}

// 获取时区
export function getLocalTimezone() {
  return localStorage.getItem('timezone') || 'Greenwich'
}

// format time
export function parsinginZone(value,format) {
  let timezone = getLocalTimezone()
  return momentTimezone(value).tz(timezone).format(format)
}

export function getBrowserLang() {
  const nav = window.navigator;
  const browserLang = (nav.language || nav.browserLanguage || '').toLowerCase();
  if (browserLang.includes('zh')) return 'zh';
  if (browserLang.includes('en')) return 'en';
  return 'zh';
}