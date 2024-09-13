<template>
  <div id="sql">
    <el-alert
      :title="$t('console.sqlWaringTip')"
      type="warning"
      show-icon
      v-if="isCondition"
      >
    </el-alert>
    <div class="sqlInput">
      <codemirror
        style="height: 100%"
        :placeholder="$t('console.sqlTip')"
        ref="sqlStr"
        @blur="blur"
        @inputRead="inputRead"
        @ready="onReady"
        @cursorActivity="cursorActivity"
        v-model="sqlStr"
        :options="cmOptions"
      ></codemirror>
    </div>
  </div>
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
  import { proprocess_sql } from "../../utils/preProcessSQL";

  _CodeMirror.resolveMode("text/x-sql").keywords = {}
  TDengineSqlKeywrods.forEach(key => (_CodeMirror.resolveMode("text/x-sql").keywords[key] = true));
  const SQLTEXT = /^(_|\.|\w)+$/g;
  export default {
    components: { codemirror },
    data() {
      return {
        requestIng: false,
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
          extraKeys: { Tab: "autocomplete", "Shift-Enter": () => this.handleSendSQL(), "Shift-Return": () => this.handleSendSQL(), 'Ctrl-/': this.toggleComment,'Cmd-/': this.toggleComment, },
          gutters: [
            "CodeMirror-lint-markers", //代码错误检测
            "CodeMirror-linenumbers",
            "CodeMirror-foldgutter", //展开收起
          ],
          autocompletion: {},
        },
        currentPosition: {
          line: 0,
          ch: 0,
        },
        comIns: null,
        isCondition: false,
      };
    },
    computed: {
      sqlStr: {
        get: function () {
          return this.$store.state.console.sqlStr;
        },
        set: function (val) {
          // console.log(this.$refs.sqlStr,this.$refs.sqlStr.codemirror.doc.lineCount(),'code----mirror----00000');
          // if(this.$refs.sqlStr.codemirror.doc.lineCount()>1){
          //   this.$refs.sqlStr.codemirror.doc.cantEdit=true
          // }else{
          //   this.$refs.sqlStr.codemirror.doc.cantEdit=false
          // }
          this.$store.commit("console/SET_SQLSTR", val);
        },
      },
      addSql() {
        return this.$store.state.console.addSql;
      },
      // sqlTip() {
      //   return this.$t('console.sqlTip')
      // }
    },
    watch: {
      addSql(newVal) {
        if (newVal) {
          this.addSqlVal(newVal);
        }
      },
      "$store.state.dbs.dbList": {
        handler(newval, oldval) {
          oldval?.forEach(item => {
            delete _CodeMirror.resolveMode("text/x-sql").keywords[item.name];
          });
          newval.forEach(item => {
            _CodeMirror.resolveMode("text/x-sql").keywords[item.name] = true;
          });
        },
        immediate: true,
      },
      sqlStr(newVal) {
        if (/^select/i.test(newVal)) {
          this.isCondition = true
        } else {
          this.isCondition = false
        }
      },
      "$i18n.locale"() {
        this.comIns.setOption('placeholder', this.$t('console.sqlTip'))
      }
    },
    mounted() {
      this.$BusOnAndAutoOff("console/sql/focus", () => {
        this.$nextTick(() => {
          this.comIns && this.comIns.focus();
        });
      });
    },
    methods: {
      toggleComment(cm) {
        const { line, ch } = cm.getCursor(); // 获取当前光标位置
        const lineContent = cm.getLine(line); // 获取当前行的内容

        if (lineContent.startsWith('--')) {
          // 如果已经是注释，取消注释
          cm.replaceRange('', { line, ch: 0 }, { line, ch: 2 });
        } else {
          // 否则添加注释
          cm.replaceRange(`--`, { line, ch: 0 }, { line, ch: 0 });
        }
      },
      onReady(ins) {
        this.comIns = ins;
      },
      blur() {
        this.currentPosition = this.comIns.getCursor();
      },
      async handleSendSQL() {
        if (this.requestIng) return;
        this.requestIng = true;
        let sqlStr = this.comIns.getSelection() || this.getSqlWithoutComments();
        let { isSendSQL, updated_sqlStr } = await proprocess_sql(sqlStr); // 预处理要执行的sql语句

        
        if (isSendSQL) {
          await this.$store.dispatch("console/sendConsoleSQL", updated_sqlStr);
        }
        this.requestIng = false;
      },
      getSqlWithoutComments() {
        const doc = this.$refs.sqlStr.codemirror.getDoc();
        let sql = "";
        for (let i = 0; i < doc.lineCount(); i++) {
          const line = doc.getLine(i);
          if (!/^-+/.test(line)) { // 判断是否为注释行
            sql += line + " "; // 拼接非注释行
          }
        }
        return sql.trim();
      },
      addSqlVal(val) {
        this.comIns.replaceRange(val, {
          line: this.currentPosition?.line,
          ch: this.currentPosition.ch,
        });
        if (this.$store.state.console.partActive == "sql") {
          this.$store.state.console.addSql = "";
          this.comIns.focus();
        }
      },
      inputRead(ins, event) {
        let text = event?.text[0];
        if (!text) return;
        if (SQLTEXT.test(text)) {
          ins.showHint();
        }
      },
      cursorActivity(ins, ev) {
        this.$store.state.console.selectedSqlStr = this.comIns.getSelection()
      }
    },
  };
</script>

<style lang="scss" scoped>
  .dbname_wrapper {
    color: #333;
    display: flex;
    flex-direction: row;
    align-items: center;
    margin-bottom: 10px;
    // cursor: pointer;
    background-color: #f5f5f5;
    height: 30px;
    .database_icon {
      width: 18px;
      height: 18px;
      flex-shrink: 0;
    }
    .dbname {
      margin-left: 10px;
    }
  }
  #sql {
    width: 100%;
    height: 20vh;
    flex-shrink: 0;
    display: flex;
    flex-direction: column;
    position: relative;
    padding: 0 15px 20px;
    &:deep(.CodeMirror) {
      height: 100%;
    }
    &:deep(.CodeMirror-placeholder) {
      color: #c0c4cc;
    }
    &:deep(.CodeMirror-linenumber) {
      text-align: left;
      padding: 0 3px 0 0;
    }
  }
  .sql-btn {
    position: absolute;
    right: 0;
    top: -50px;
    z-index: 2;
  }
  .sqlInput {
    flex: 1;
    height: 200px;
    margin-top: 5px;
    overflow: auto;
  }
  ::v-deep .CodeMirror {
    font-size: 16px;
    height: 80%;
  }
  ::v-deep .CodeMirror-scroll {
    height: 80% !important;
  }
  .button {
    line-height: 20px;
    border-radius: 4px;
    border: none;
    font-size: 14px;
    font-weight: 400;
    color: #ffffff;
    &.disabled {
      background: #ccc;
      color: #fff;
      cursor: not-allowed;
    }
  }
  .button-run {
    background: $color-primary;
  }
  .button-run:hover {
    background: #666666;
  }
  .add_favorite_btn {
    @extend .flexCenter;
    margin-left: 10px;
    background-color: #67c23a;
  }

  .favorite_icon {
    width: 15px;
    height: 15px;
    color: #fff;
  }
  ::v-deep .CodeMirror-lint-markers {
    display: none;
  }

  ::v-deep .CodeMirror-lines {
    padding: 4px 4px;
  }
</style>
