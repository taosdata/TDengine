import type { Directive, App } from 'vue';
import hljs from 'highlight.js';
import 'highlight.js/styles/atom-one-light.css'; //样式
import * as utils from '@/utils';
import { PermissionMap } from '@/const.ts';
import store from '@/store';
import i18n from '@/lang/index.ts';
import { debounce } from 'lodash-es';
//长按
const longpress: Directive = {
  beforeMount: function (el, binding) {
    if (typeof binding.value !== 'function') {
      throw 'callback must be a function';
    }
    // 定义变量
    let pressTimer = null;
    // 创建计时器（ 2秒后执行函数 ）
    const start = e => {
      if (e.type === 'click' && e.button !== 0) {
        return;
      }
      if (pressTimer === null) {
        pressTimer = setTimeout(() => {
          handler();
        }, 2000);
      }
    };
    // 取消计时器
    const cancel = () => {
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
    el.addEventListener('mousedown', start);
    el.addEventListener('touchstart', start);
    // 取消计时器
    el.addEventListener('click', cancel);
    el.addEventListener('mouseout', cancel);
    el.addEventListener('touchend', cancel);
    el.addEventListener('touchcancel', cancel);
  },
  // 当传进来的值更新的时候触发
  updated(el, { value }) {
    el.$value = value;
  },
  // 指令与元素解绑的时候，移除事件绑定
  unmounted(el) {
    el.removeEventListener('click', el.handler);
  }
};

const findEle = (parent, type) => {
  return parent.tagName.toLowerCase() === type ? parent : parent.querySelector(type);
};

const trigger = (el, type) => {
  const e = document.createEvent('HTMLEvents');
  e.initEvent(type, true, true);
  el.dispatchEvent(e);
};
// 只允许输入数字和字母
const emoji: Directive = {
  beforeMount: function (el) {
    // 正则规则可根据需求自定义
    const regRule = /[^u4E00-u9FA5|d|a-zA-Z|rns,.?!，。？！…—&$=()-+/*{}[]]|s/g;
    const $inp = findEle(el, 'input');
    el.$inp = $inp;
    $inp.handle = function () {
      const val = $inp.value;
      $inp.value = val.replace(regRule, '');

      trigger($inp, 'input');
    };
    $inp.addEventListener('keyup', $inp.handle);
  },
  unmounted: function (el) {
    el.$inp.removeEventListener('keyup', el.$inp.handle);
  }
};

function checkPermission(key) {
  return PermissionMap[key];
}
// 检查权限
const permission: Directive = {
  mounted: function (el) {
    const permission = store.getters.role; // 获取到 v-permission的值
    if (permission) {
      const hasPermission = checkPermission(permission);
      if (!hasPermission) {
        // 没有权限 移除Dom元素
        el.parentNode && el.parentNode.removeChild(el);
      }
    }
  }
};

// 代码高亮
const highlight: Directive = {
  mounted(el, { value, modifiers: { noCopy } }) {
    // 当包含value的时候说明代码中含有变量
    el.hljsBlock = el.querySelectorAll('code')[0];
    el.hightFn = value => {
      el.hljsBlock.innerHTML = hljs.highlightAuto(value, [el.customLang]).value;
    };
    // 查看语言类型
    el.customLang =
      el.hljsBlock.className
        .split(' ')
        ?.find(item => item.includes('language-'))
        ?.split('-')[1] || '';
    // 如果value存在才会在后面更新，如果不存在就不更新
    if (value) {
      el.isUpdate = true;
      el.hightFn(value);
    } else {
      el.hightFn(el.hljsBlock.innerText);
    }
    // 给pre标签添加复制按钮 如果没有code标签就不添加
    if (el.hljsBlock && !noCopy) {
      const btn = document.createElement('button');
      btn.innerHTML = `<i class='CopyDocument'></i> ${i18n.global.t('copy')}`;
      btn.classList.add('copy-btn');
      el.classList.add('pre-code');
      btn.onclick = e => {
        e.stopPropagation();
        utils.copy(el.hljsBlock.innerText);
      };
      el.appendChild(btn);
    }
  },
  updated(el, { value }) {
    el.isUpdate && el.hightFn(value);
  },
  unmounted(el) {
    el.hljsBlock = null;
  }
};
// 加载更多
const loadMore: Directive = {
  beforeMount(el, binding, vnode) {
    const { expand, immediate } = binding.modifiers;
    // 使用更丰富的功能，支持父组件的指令作用在指定的子组件上
    if (expand) {
      /**
       * target 目标DOM节点的类名
       * distance 减少触发加载的距离阈值，单位为px
       * func 触发的方法
       * func 横向滚动触发的方法
       * delay 防抖时延，单位为ms
       * load-more-disabled 是否禁用无限加载
       */
      const { target, distance = 0, func, func1, delay = 200 } = binding.value;
      if (typeof target !== 'string') return;
      const targetEl = el.querySelector(target);
      if (!targetEl) {
        return;
      }
      binding.handler = debounce(function () {
        const { scrollTop, scrollHeight, clientHeight, scrollLeft, scrollWidth, clientWidth } = targetEl;
        let disabled = el.getAttribute('load-more-disabled');
        disabled = vnode[disabled] || disabled;
        if (scrollWidth <= scrollLeft + clientWidth + distance) {
          if (disabled) return;
          func1 && func1();
        }
        if (scrollHeight <= scrollTop + clientHeight + distance) {
          if (disabled) return;
          func && func();
        }
      }, delay);

      targetEl.addEventListener('scroll', binding.handler);
    } else {
      binding.handler = debounce(function () {
        const { scrollTop, scrollHeight, clientHeight } = el;
        if (scrollHeight === scrollTop + clientHeight) {
          binding.value && binding.value();
        }
      }, 200);
      el.addEventListener('scroll', binding.handler);
    }
    immediate && binding.handler();
  },
  unmounted(el, binding) {
    const { arg } = binding;
    // 使用更丰富的功能，支持父组件的指令作用在指定的子组件上
    if (arg === 'expand') {
      /**
       * target 目标DOM节点的类名
       * offset 触发加载的距离阈值，单位为px
       * method 触发的方法
       * delay 防抖时延，单位为ms
       */
      const { target } = binding.value;
      if (typeof target !== 'string') return;
      let targetEl = el.querySelector(target);
      targetEl && targetEl.removeEventListener('scroll', binding.handler);
      targetEl = null;
    } else {
      el.removeEventListener('scroll', binding.handler);
      el = null;
    }
  }
};
const directives: Record<string, any> = {
  longpress,
  permission,
  emoji,
  highlight,
  loadMore
};

export const registerDirective = (app: App) => {
  Object.keys(directives).forEach(key => {
    app.directive(key, directives[key]);
  });
};
