import mavon from 'mavon-editor';
import { Component } from 'vue';
import hljs from 'highlight.js';

const markdownIt = (
  mavon.mavonEditor as Component & {
    getMarkdownIt: () => any;
  }
).getMarkdownIt();

markdownIt.set({
  highlight: function (str: string, lang: string) {
    if (lang && hljs.getLanguage(lang)) {
      try {
        return hljs.highlight(str, { language: lang }).value;
      } catch (err) {
        console.log(err);
      }
    }

    return ''; // use external default escaping
  }
});

// 保存默认的图片渲染规则
const defaultRender = markdownIt.renderer.rules.image;
markdownIt.renderer.rules.image = function (tokens: any, idx: any, options: any, env: any, self: any) {
  const result = defaultRender(tokens, idx, options, env, self);
  return result.replace(/src=['"](\/(api|app))/, function (match: any, p1: any) {
    return match.replace(p1, p1 == '/api' ? '/app' : '/api');
  });
};

export default markdownIt;
