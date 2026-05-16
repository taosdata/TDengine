import { json2csv, Json2CsvOptions } from 'json-2-csv';

export const UTF8_BOM = '\ufeff';
export const CSV_MIME_TYPE = 'text/csv;charset=utf-8';

export function withCsvUtf8Bom(csv: string) {
  return csv.startsWith(UTF8_BOM) ? csv : UTF8_BOM + csv;
}

export function createUtf8CsvBlob(csv: string) {
  return new Blob(csv.startsWith(UTF8_BOM) ? [csv] : [UTF8_BOM, csv], {
    type: CSV_MIME_TYPE
  });
}

let pageHead: null | HTMLHeadElement = null;
function getPageHead() {
  return (pageHead = pageHead || document.getElementsByTagName('head')[0] || document.documentElement || document.body);
}

type ScriptAndLinkElementProps = Partial<HTMLScriptElement & HTMLLinkElement>;

// 创建一个新的接口，包含所有属性
interface IAttrMaps extends ScriptAndLinkElementProps {
  [key: string]: any;
}

interface ResourceResult {
  script: HTMLScriptElement;
  link: HTMLLinkElement;
}
/**
 * 通过自定义方式加载资源
 * @param {String} tagName 加载资源用的dom节点名称，比如 script / link
 * @param {String} triggerAttr 加载资源用的dom属性，比如 src / href
 * @param {String} file 加载的资源路径
 * @param {String} attrsMap 附加dom上的其他属性
 * @returns {Promise} 返回资源加载结果Promise
 */
export function loadResource<T extends 'script' | 'link'>(
  tagName: T,
  triggerAttr: T extends 'script' ? 'src' : 'href',
  file: string,
  attrsMap: IAttrMaps = {}
) {
  return new Promise<ResourceResult[T]>(function (resolve, reject) {
    const head: HTMLHeadElement = getPageHead();
    const tag = document.createElement(tagName) as ResourceResult[T];
    for (const attr in attrsMap) {
      tag.setAttribute(attr, attrsMap[attr] as string);
    }
    tag.onload = function () {
      tag.onload = null;
      resolve(tag);
    };
    tag.onerror = reject;
    (tag as any)[triggerAttr] = file;
    head.appendChild(tag);
  });
}

/**
 * 加载一个js文件
 * @param {String} js 需要加载的js资源路径
 */
export const loadJS = (js: string) =>
  loadResource('script', 'src', js, {
    type: 'text/javascript'
  });

/**
 * 加载一个css文件
 * @param {String} css 需要加载的css资源路径
 */
export const loadCss = (css: string) =>
  loadResource('link', 'href', css, {
    type: 'text/css',
    rel: 'stylesheet'
  });

/**
 * 加载一个图片
 * @param {String} img 需要加载的图片资源路径
 */
// export const loadImage = img => loadResource('img', 'src', img, {
//   style: 'position:absolute;left:-99999px;top:-99999px;z-index:-99'
// });
export const loadImage = (imgUrl: string) =>
  new Promise((resolve, reject) => {
    const img = new Image();
    // 监听加载
    img.onload = function () {
      resolve({
        width: img.width,
        height: img.height
      });
    };
    img.onerror = function () {
      reject(null);
    };
    img.src = imgUrl;
  });

// base ==> blob
export const base64ToBlob = (base64: string): Blob => {
  const arr = base64.split(',');
  const typeItem = arr[0];
  const mime = typeItem.match(/:(.*?);/)![1];
  const bstr = atob(arr[1]);
  let n = bstr.length;
  const u8arr = new Uint8Array(n);
  while (n--) {
    u8arr[n] = bstr.charCodeAt(n);
  }
  const blob = new Blob([u8arr], {
    type: mime
  });
  return blob;
};

// url ==> base64
export function urlToBase64(url: string, mineType?: string): Promise<string> {
  return new Promise((resolve, reject) => {
    let canvas = document.createElement('CANVAS') as Nullable<HTMLCanvasElement>;
    const ctx = canvas!.getContext('2d');

    const img = new Image();
    img.crossOrigin = '';
    img.onload = function () {
      if (!canvas || !ctx) {
        return reject(new Error('错误'));
      }
      canvas.height = img.height;
      canvas.width = img.width;
      ctx.drawImage(img, 0, 0);
      const dataURL = canvas.toDataURL(mineType || 'image/png');
      canvas = null;
      resolve(dataURL);
    };
    img.src = url;
  });
}

// blob 转 file
export async function blobToFile(Blobs: Blob[] = [], fileName = 'test.zip', fileType = 'application/zip') {
  return new File(Blobs, fileName, {
    type: fileType
  });
}
// base64 转 file
export function base64ToFile(url: string, fileName: string) {
  const arr = url.split(',');
  const mime = arr[0].match(/:(.*?);/)![1];
  const bstr = atob(arr[1]);
  let n = bstr.length;
  const u8arr = new Uint8Array(n);
  while (n--) {
    u8arr[n] = bstr.charCodeAt(n);
  }
  return new File([u8arr], fileName, {
    type: mime
  });
}

/**
 * @description 下载 blob 文件
 * @author YaBin
 * @date 04/07/2024
 * @export
 * @param {BlobPart} data
 * @param {string} filename
 * @param {string} [mime]
 * @param {BlobPart} [bom]
 */
export function downloadByData(data: BlobPart, filename: string, mime?: string, bom?: BlobPart) {
  const blobData = typeof bom !== 'undefined' ? [bom, data] : [data];
  const blob = new Blob(blobData, { type: mime || 'application/octet-stream' });

  const blobURL = window.URL.createObjectURL(blob);
  downloadByUrl(blobURL, filename);
  window.URL.revokeObjectURL(blobURL);
}

// base64 下载
export function downloadByBase64(buf: string, filename: string, mime?: string, bom?: BlobPart) {
  const base64Buf = base64ToBlob(buf);
  downloadByData(base64Buf, filename, mime, bom);
}

/**
 * URL 下载文件
 * @export
 * @param {string} url
 * @param {string} [filename='data']
 * @param {boolean} [newTabPage]
 */
export function downloadByUrl(url: string, filename = 'data', target = '_blank') {
  // 创建隐藏的可下载链接
  const eleLink = document.createElement('a');
  eleLink.setAttribute('target', target);
  eleLink.download = filename;
  eleLink.style.display = 'none';
  eleLink.href = url;
  // 触发点击
  document.body.appendChild(eleLink);
  eleLink.click();
  // 然后移除
  document.body.removeChild(eleLink);
}

/**
 * blob 转文本
 * @export
 * @param {Blob} blob
 * @return {Promise<string>}
 */
export function blobToText(blob: Blob): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onloadend = () => resolve(reader.result as string);
    reader.onerror = reject;
    reader.readAsText(blob, 'utf-8');
  });
}

/**
 * @description 导出为csv文件
 * @author YaBin
 * @date 04/07/2024
 * @export
 * @param {any[]} data
 * @param {string} [filename='data']
 * @return {*}
 */
export function exportCsv(data: any[], options?: Json2CsvOptions, filename = 'data') {
  downloadByData(json2csv(data, options), filename + '.csv', CSV_MIME_TYPE, UTF8_BOM);
}
