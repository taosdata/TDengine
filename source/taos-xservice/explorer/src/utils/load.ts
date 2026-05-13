let pageHead;
function getPageHead () {
  return pageHead = pageHead ||
  document.getElementsByTagName('head')[0] ||
  document.documentElement ||
  document.body;
}

/**
 * 通过自定义方式加载资源
 * @param {String} tagName 加载资源用的dom节点名称，比如 script / link
 * @param {String} triggerAttr 加载资源用的dom属性，比如 src / href
 * @param {String} file 加载的资源路径
 * @param {String} attrsMap 附加dom上的其他属性
 * @returns {Promise} 返回资源加载结果Promise
 */
export function loadResource (tagName, triggerAttr, file, attrsMap = {}) {
  return new Promise(function (resolve, reject) {
    let head = getPageHead();
    let tag = document.createElement(tagName);
    attrsMap.charset = attrsMap.charset || 'UTF-8';
    for (const attr in attrsMap) {
      tag[attr] = attrsMap[attr];
    }
    let done = false;
    tag.onload = tag.onreadystatechange = function () {
      if (
        !done &&
        (!this.readyState ||
          {
            loaded: 1,
            complete: 1
          }[this.readyState])
      ) {
        // 重置状态
        done = true;
        tag.onload = tag.onreadystatechange = null;
        this.parentNode.removeChild(this);
        // 释放引用，内存回收
        head = tag = null;
        // Callback
        resolve();
      }
    };
    tag.onerror = reject;
    tag[triggerAttr] = file;
    head.appendChild(tag, head.lastChild);
  });
}

/**
 * 加载一个js文件
 * @param {String} js 需要加载的js资源路径
 */
export const loadJS = js => loadResource('script', 'src', js, {
  type: 'text/javascript'
});

/**
 * 加载一个css文件
 * @param {String} css 需要加载的css资源路径
 */
export const loadCss = css => loadResource('link', 'href', css, {
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
export const loadImage = imgUrl => new Promise((resolve, reject) => {
  const img = new Image();
  // 防止img在加载中被强制内存回收的风险，强制挂接在window上
  const randomProp = 'imageLoad'+ Math.random().toString(16).slice(2);
  window[randomProp] = img;
  // 监听加载
  img.onload = function () {
    delete window[randomProp];
    resolve({
      width: img.width,
      height: img.height
    });
  };
  img.onerror = function () {
    delete window[randomProp];
    reject(null);
  };
  img.src = imgUrl;
});

