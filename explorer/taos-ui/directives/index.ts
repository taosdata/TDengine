import hljs from 'highlight.js';
import 'highlight.js/styles/github.css'; // 你可以选择其他样式
import { DirectiveBinding } from 'vue';
import { copy } from 'utils';
import { t } from 'locales';
import { debounce } from 'lodash-es';

interface HighlightElement extends HTMLElement {
  hljsBlock?: HTMLElement;
  hightFn?: (value: string) => void;
  customLang: string;
  isUpdate?: boolean;
}
// 代码高亮
export const highlight = {
  mounted(el: HighlightElement, binding: DirectiveBinding) {
    const {
      value,
      modifiers: { noCopy }
    } = binding;

    el.hljsBlock = el.querySelectorAll('code')[0] as HTMLElement;
    if (!el.hljsBlock) return;
    const customLang =
      el.hljsBlock.className
        .split(' ')
        ?.find(item => item.includes('language-'))
        ?.split('-')[1] || '';
    const langParameter = customLang ? [customLang] : undefined;
    el.hightFn = (value: string) => {
      el.hljsBlock!.innerHTML = hljs.highlightAuto(value, langParameter).value;
    };

    if (value) {
      el.isUpdate = true;
      el.hightFn(value);
    } else {
      el.hightFn(el.hljsBlock.innerText);
    }

    if (el.hljsBlock && !noCopy) {
      const btn = document.createElement('button');
      btn.innerHTML = t('common.copy');
      btn.classList.add('copy-btn');
      el.classList.add('pre-code');
      btn.onclick = (e: MouseEvent) => {
        e.stopPropagation();
        if (!el.hljsBlock) return;
        copy(el.hljsBlock.innerText);
      };
      el.appendChild(btn);
    }
  },
  updated(el: HighlightElement, binding: DirectiveBinding) {
    const { value } = binding;
    if (el.hljsBlock && el.isUpdate && value) {
      el.hightFn!(value);
    }
  }
};
interface LoadMoreEl extends HTMLElement {
  handler?: () => void;
}
// 加载更多
export const loadMore = {
  mounted(el: LoadMoreEl, binding: DirectiveBinding, vnode: any) {
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
      const { target, distance = 0, func, func1, delay = 200 } = binding.value;
      if (typeof target !== 'string') return;
      const targetEl = el.querySelector(target);
      if (!targetEl) {
        console.log('找不到容器');
        return;
      }

      el.handler = debounce(function () {
        const { scrollTop, scrollHeight, clientHeight, scrollLeft, scrollWidth, clientWidth } = targetEl;
        let disabled = el.getAttribute('load-more-disabled');
        disabled = vnode.props?.disabled || disabled;

        // 水平滚动检查
        if (scrollWidth <= scrollLeft + clientWidth + distance) {
          if (disabled) return;
          func1?.(); // 使用可选链
        }

        // 垂直滚动检查
        if (scrollHeight <= scrollTop + clientHeight + distance) {
          if (disabled) return;
          func?.(); // 使用可选链
        }
      }, delay);

      targetEl.addEventListener('scroll', el.handler);
    } else {
      el.handler = debounce(function () {
        const { scrollTop, scrollHeight, clientHeight } = el;
        if (scrollHeight <= scrollTop + clientHeight + 100) {
          binding.value?.(); // 使用可选链
        }
      }, 200);
      el.addEventListener('scroll', el.handler);
    }
    immediate && el.handler();
  },

  unmounted(el: LoadMoreEl, binding: DirectiveBinding) {
    const { expand } = binding.modifiers;

    if (expand) {
      const { target } = binding.value;
      if (typeof target !== 'string') return;

      const targetEl = el.querySelector(target);
      targetEl?.removeEventListener('scroll', el.handler!); // 使用可选链
    } else {
      el.removeEventListener('scroll', el.handler!);
    }
  }
};
