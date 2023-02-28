<template>
  <codemirror class="custom-codemirror" :placeholder="placeholder" ref="sqlStr" v-model="sqlStr" :options="cmOptions"></codemirror>
</template>

<script>
  import { TDengineSqlKeywrods } from "@/const";
  import _CodeMirror from "codemirror";
  import { codemirror } from "vue-codemirror";
  import "codemirror/lib/codemirror.css";
  import "codemirror/theme/eclipse.css";
  import "codemirror/mode/sql/sql.js";
  import "codemirror/addon/hint/show-hint.js";
  import "codemirror/addon/hint/show-hint.css";
  import "codemirror/addon/search/match-highlighter.js";
  import "codemirror/addon/hint/sql-hint.js";
  import "codemirror/addon/edit/closebrackets.js";
  import "codemirror/addon/lint/lint.css";
  import "codemirror/addon/lint/lint.js";
  // import "codemirror/addon/lint/sql-lint";
  import "codemirror/addon/edit/closetag.js";
  import "codemirror/addon/edit/matchtags.js";
  import "codemirror/addon/edit/matchbrackets.js";
  import "codemirror/addon/selection/active-line.js";
  import "codemirror/addon/search/jump-to-line.js";
  import "codemirror/addon/dialog/dialog.js";
  import "codemirror/addon/dialog/dialog.css";
  import "codemirror/addon/search/searchcursor.js";
  import "codemirror/addon/search/search.js";
  import "codemirror/addon/display/autorefresh.js";
  import "codemirror/addon/selection/mark-selection.js";
  import "codemirror/addon/search/match-highlighter.js";
  import "codemirror/addon/display/placeholder.js";

  TDengineSqlKeywrods.forEach(key => (_CodeMirror.resolveMode("text/x-sql").keywords[key] = true));
  export default {
    props: {
      value: {
        type: String,
        default: "",
      },
      placeholder: {
        type: String,
        default: "",
      },
    },
    components: { codemirror },
    data() {
      return {
        cmOptions: {
          tabSize: 2,
          mode: "text/x-sql",
          theme: "eclipse",
          lineNumbers: true,
          spellcheck: true, //拼写检查
          line: true,
          hintOptions: { completeSingle: false },
          viewportMargin: 2,
          autofocus: false,
          showCursorWhenSelecting: true,
          extraKeys: { Tab: "autocomplete", "Shift-Enter": () => this.handleSendSQL(), "Shift-Return": () => this.handleSendSQL() },
          gutters: [
            "CodeMirror-lint-markers", //代码错误检测
            "CodeMirror-linenumbers",
            "CodeMirror-foldgutter", //展开收起
          ],
          autocompletion: {},
        },
      };
    },
    computed: {
      sqlStr: {
        get() {
          return this.value;
        },
        set(val) {
          this.$emit("input", val);
        },
      },
    },

    created() {},
    mounted() {},
    methods: {},
  };
</script>

<style lang="scss">
  .custom-codemirror ::v-deep .CodeMirror-placeholder {
    color: #c0c4cc;
  }
</style>
