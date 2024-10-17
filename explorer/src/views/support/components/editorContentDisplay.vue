<template>
  <div class="markdown-body" v-html="html"></div>
</template>

<script>
  import "github-markdown-css/github-markdown-light.css";
  import hljs from "highlight.js";
  import "highlight.js/styles/atom-one-light.css";
  import { marked } from "marked";
  // 配置 marked
  const renderer = new marked.Renderer();
  marked.setOptions({
    gfm: true,
    renderer: renderer, // 这个是必须填写的
    pedantic: false, // 只解析符合Markdown定义的，不修正Markdown的错误
    sanitize: false, // 原始输出，忽略HTML标签
    tables: true, // 支持Github形式的表格，必须打开gfm选项
    breaks: false, // 支持Github换行符，必须打开gfm选项
    smartLists: true, // 优化列表输出
    smartypants: false,
    // 高亮显示规则 ，这里使用highlight.js来完成
    highlight: function (code) {
      return hljs.highlightAuto(code).value;
    },
  });
  export default {
    props: {
      content: {
        type: String,
        default: "",
      },
    },
    computed: {
      html() {
        return marked(this.content);
      },
    },
  };
</script>

<style scoped>
  .ql-display {
    margin: 20px 0;
  }
  .ql-container.ql-snow {
    border: none;
  }
</style>
