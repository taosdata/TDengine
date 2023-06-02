import hljs from "highlight.js";
import "highlight.js/styles/atom-one-light.css"; //样式
import * as utils from "@/utils";
import { PermissionMap } from "@/const";
import store from "@/store";
import i18n from "@/lang";
//长按
const longpress = {
  bind: function (el, binding) {
    if (typeof binding.value !== "function") {
      throw "callback must be a function";
    }
    // 定义变量
    let pressTimer = null;
    // 创建计时器（ 2秒后执行函数 ）
    let start = e => {
      if (e.type === "click" && e.button !== 0) {
        return;
      }
      if (pressTimer === null) {
        pressTimer = setTimeout(() => {
          handler();
        }, 2000);
      }
    };
    // 取消计时器
    let cancel = () => {
      if (pressTimer !== null) {
        clearTimeout(pressTimer);
        pressTimer = null;
      }
    };
    // 运行函数
    const handler = e => {
      binding.value(e);
    };
    // 添加事件监听器
    el.addEventListener("mousedown", start);
    el.addEventListener("touchstart", start);
    // 取消计时器
    el.addEventListener("click", cancel);
    el.addEventListener("mouseout", cancel);
    el.addEventListener("touchend", cancel);
    el.addEventListener("touchcancel", cancel);
  },
  // 当传进来的值更新的时候触发
  componentUpdated(el, { value }) {
    el.$value = value;
  },
  // 指令与元素解绑的时候，移除事件绑定
  unbind(el) {
    el.removeEventListener("click", el.handler);
  },
};
// 防抖
const debounce = {
  inserted: function (el, binding) {
    let timer;
    el.addEventListener("keyup", () => {
      if (timer) {
        clearTimeout(timer);
      }
      timer = setTimeout(() => {
        binding.value();
      }, 1000);
    });
  },
};

let findEle = (parent, type) => {
  return parent.tagName.toLowerCase() === type ? parent : parent.querySelector(type);
};

const trigger = (el, type) => {
  const e = document.createEvent("HTMLEvents");
  e.initEvent(type, true, true);
  el.dispatchEvent(e);
};
// 只允许输入数字和字母
const emoji = {
  bind: function (el) {
    // 正则规则可根据需求自定义
    var regRule = /[^u4E00-u9FA5|d|a-zA-Z|rns,.?!，。？！…—&$=()-+/*{}[]]|s/g;
    let $inp = findEle(el, "input");
    el.$inp = $inp;
    $inp.handle = function () {
      let val = $inp.value;
      $inp.value = val.replace(regRule, "");

      trigger($inp, "input");
    };
    $inp.addEventListener("keyup", $inp.handle);
  },
  unbind: function (el) {
    el.$inp.removeEventListener("keyup", el.$inp.handle);
  },
};
// 图片懒加载
export const LazyLoad = {
  // install方法
  install(Vue) {
    const defaultSrc = "@/assets/fonts/svg/logo.svg";
    Vue.directive("lazy", {
      bind(el, binding) {
        LazyLoad.init(el, binding.value, defaultSrc);
      },
      inserted(el) {
        if (IntersectionObserver) {
          LazyLoad.observe(el);
        } else {
          LazyLoad.listenerScroll(el);
        }
      },
    });
  },
  // 初始化
  init(el, val, def) {
    el.setAttribute("data-src", val);
    el.setAttribute("src", def);
  },
  // 利用IntersectionObserver监听el
  observe(el) {
    var io = new IntersectionObserver(entries => {
      const realSrc = el.dataset.src;
      if (entries[0].isIntersecting) {
        if (realSrc) {
          el.src = realSrc;
          el.removeAttribute("data-src");
        }
      }
    });
    io.observe(el);
  },
  // 监听scroll事件
  listenerScroll(el) {
    const handler = LazyLoad.throttle(LazyLoad.load, 300);
    LazyLoad.load(el);
    window.addEventListener("scroll", () => {
      handler(el);
    });
  },
  // 加载真实图片
  load(el) {
    const windowHeight = document.documentElement.clientHeight;
    const elTop = el.getBoundingClientRect().top;
    const elBtm = el.getBoundingClientRect().bottom;
    const realSrc = el.dataset.src;
    if (elTop - windowHeight < 0 && elBtm > 0) {
      if (realSrc) {
        el.src = realSrc;
        el.removeAttribute("data-src");
      }
    }
  },
  // 节流
  throttle(fn, delay) {
    let timer;
    let prevTime;
    return function (...args) {
      const currTime = Date.now();
      const context = this;
      if (!prevTime) prevTime = currTime;
      clearTimeout(timer);

      if (currTime - prevTime > delay) {
        prevTime = currTime;
        fn.apply(context, args);
        clearTimeout(timer);
        return;
      }

      timer = setTimeout(function () {
        prevTime = Date.now();
        timer = null;
        fn.apply(context, args);
      }, delay);
    };
  },
};

function checkPermission(key) {
  return PermissionMap[key];
}
// 检查权限
const permission = {
  inserted: function (el) {
    let permission = store.getters.role; // 获取到 v-permission的值
    if (permission) {
      let hasPermission = checkPermission(permission);
      if (!hasPermission) {
        // 没有权限 移除Dom元素
        el.parentNode && el.parentNode.removeChild(el);
      }
    }
  },
};

// 添加背景水印
function addWaterMarker(str, parentNode, font, textColor) {
  // 水印文字，父元素，字体，文字颜色
  var can = document.createElement("canvas");
  parentNode.appendChild(can);
  can.width = 200;
  can.height = 150;
  can.style.display = "none";
  var cans = can.getContext("2d");
  cans.rotate((-20 * Math.PI) / 180);
  cans.font = font || "16px Microsoft JhengHei";
  cans.fillStyle = textColor || "rgba(180, 180, 180, 0.3)";
  cans.textAlign = "left";
  cans.textBaseline = "Middle";
  cans.fillText(str, can.width / 10, can.height / 2);
  parentNode.style.backgroundImage = "url(" + can.toDataURL("image/png") + ")";
}

const waterMarker = {
  bind: function (el, binding) {
    addWaterMarker(binding.value.text, el, binding.value.font, binding.value.textColor);
  },
};

// 代码高亮
const highlight = {
  inserted(el, { value, modifiers: { noCopy } }) {
    // 当包含value的时候说明代码中含有变量
    el.hljsBlock = el.querySelectorAll("code")[0];
    el.hightFn = value => {
      el.hljsBlock.innerHTML = hljs.highlightAuto(value, [el.customLang]).value;
    };
    // 查看语言类型
    el.customLang =
      el.hljsBlock.className
        .split(" ")
        ?.find(item => item.includes("language-"))
        ?.split("-")[1] || "";
    // 如果value存在才会在后面更新，如果不存在就不更新
    if (value) {
      el.isUpdate = true;
      el.hightFn(value);
    } else {
      el.hightFn(el.hljsBlock.innerText);
    }
    // 给pre标签添加复制按钮 如果没有code标签就不添加
    if (el.hljsBlock && !noCopy) {
      let btn = document.createElement("button");
      btn.innerHTML = `<i class='el-icon-copy-document'></i> ${i18n.t('copy')}`;
      btn.classList.add("copy-btn");
      el.classList.add("pre-code");
      btn.onclick = e => {
        e.stopPropagation();
        utils.copy(el.hljsBlock.innerText);
      };
      el.appendChild(btn);
    }
  },
  componentUpdated(el, { value }) {
    el.isUpdate && el.hightFn(value);
  },
  unbind(el) {
    el.hljsBlock = null;
  },
};
// 加载更多
const loadMore = {
  bind(el, binding, vnode) {
    const { expand, immediate } = binding.modifiers;
    // 使用更丰富的功能，支持父组件的指令作用在指定的子组件上
    if (expand) {
      /**
       * target 目标DOM节点的类名
       * distance 减少触发加载的距离阈值，单位为px
       * func 触发的方法
       * delay 防抖时延，单位为ms
       * load-more-disabled 是否禁用无限加载
       */
      let { target, distance = 0, func, delay = 200 } = binding.value;
      if (typeof target !== "string") return;
      let targetEl = el.querySelector(target);
      if (!targetEl) {
        return;
      }
      binding.handler = utils.debounce(function () {
        const { scrollTop, scrollHeight, clientHeight } = targetEl;
        let disabled = el.getAttribute("load-more-disabled");
        disabled = vnode[disabled] || disabled;
        if (scrollHeight <= scrollTop + clientHeight + distance) {
          if (disabled) return;
          func && func();
        }
      }, delay);

      targetEl.addEventListener("scroll", binding.handler);
    } else {
      binding.handler = utils.debounce(function () {
        const { scrollTop, scrollHeight, clientHeight } = el;
        if (scrollHeight === scrollTop + clientHeight) {
          binding.value && binding.value();
        }
      }, 200);
      el.addEventListener("scroll", binding.handler);
    }
    immediate && binding.handler();
  },
  unbind(el, binding) {
    let { arg } = binding;
    // 使用更丰富的功能，支持父组件的指令作用在指定的子组件上
    if (arg === "expand") {
      /**
       * target 目标DOM节点的类名
       * offset 触发加载的距离阈值，单位为px
       * method 触发的方法
       * delay 防抖时延，单位为ms
       */
      const { target } = binding.value;
      if (typeof target !== "string") return;
      let targetEl = el.querySelector(target);
      targetEl && targetEl.removeEventListener("scroll", binding.handler);
      targetEl = null;
    } else {
      el.removeEventListener("scroll", binding.handler);
      el = null;
    }
  },
};
const directives = {
  longpress,
  debounce,
  waterMarker,
  permission,
  emoji,
  highlight,
  loadMore,
};
export default {
  install(Vue) {
    Object.keys(directives).forEach(key => {
      Vue.directive(key, directives[key]);
    });
  },
};
