<template>
  <div class="sql-code-editor">
    <div class="editor-area">
      <SqlEditor
        v-model="sqlStr"
        height="100%"
        :db-list="dbList"
        :placeholder="t('explorer.sqlCodeTip')"
        @blur="handleBlur"
        @ready="handleReady"
        @execute="handleExecute"
      ></SqlEditor>
    </div>
    <div class="float-in-sql-editor">
      <el-alert v-if="isSelectSql" :title="t('explorer.resultLimitWarningTip')" type="warning" show-icon> </el-alert>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { format } from 'sql-formatter';
import { validSqlIsSelect } from 'utils/validate';
import { getSqlProvider } from '../model/useExplorer';
import SqlEditor from 'components/SqlCodeEditor/index.vue';
import { t } from 'locales';
import { EditorView } from '@codemirror/view';
import { addSqlCodeEvent, editorFocusEvent, dbList } from './utils';

const { sqlStr, executeSql } = getSqlProvider();
const isSelectSql = computed(() => validSqlIsSelect(sqlStr.value));
const editoIns = ref<EditorView | null>(null);
let currentPosition = {
  line: 0,
  ch: 0
};

defineExpose({
  handleExecute,
  handleFormat
});

addSqlCodeEvent.on((code: string) => {
  addCodeAtPosition(code);
});

editorFocusEvent.on(() => {
  editoIns.value?.focus();
});

function handleExecute() {
  const selection = getCurrentSelection();
  executeSql(selection || sqlStr.value);
}

function handleFormat() {
  if (!editoIns.value) return;

  const { from, to } = editoIns.value.state.selection.main;
  if (from < to) {
    const selection = editoIns.value.state.sliceDoc(from, to);
    const formattedSql = format(selection, { language: 'mysql' });
    editoIns.value.dispatch({
      changes: { from, to, insert: formattedSql }
    });
  } else {
    const formattedSql = format(sqlStr.value, { language: 'mysql' });
    sqlStr.value = formattedSql;
  }
}

function handleReady(payload: Recordable) {
  editoIns.value = payload.view;
}

function getCurrentSelection() {
  if (!editoIns.value) return;
  const { from, to } = editoIns.value.state.selection.main;
  return editoIns.value.state.sliceDoc(from, to);
}
function handleBlur() {
  if (!editoIns.value) return;
  const view = editoIns.value;
  const cursorPos = view.state.selection.main.head;
  const line = view.state.doc.lineAt(cursorPos);
  currentPosition = {
    line: line.number,
    ch: cursorPos - line.from
  };
}
function addCodeAtPosition(code: string) {
  if (!editoIns.value) return;
  const line = editoIns.value.state.doc.line(currentPosition.line || 1);
  const pos = line.from + currentPosition.ch;
  editoIns.value.dispatch({
    changes: { from: pos, insert: code }
  });
}
</script>

<style scoped lang="scss">
.sql-code-editor {
  position: relative;
  display: flex;
  flex: 1;
  flex-direction: column;
  width: 100%;
  height: 100%;
  padding: 0;
  margin-top: -20px;
  overflow: auto;

  .float-in-sql-editor {
    position: absolute;
    right: 10px;
    bottom: 10px;
    left: 60px;
    z-index: 10;
  }

  /* stylelint-disable-next-line selector-class-pattern */
  &:deep(.CodeMirror) {
    height: 100%;
    font-family: 'IBM Plex Mono', monospace;
  }

  /* stylelint-disable-next-line selector-class-pattern */
  &:deep(.CodeMirror-placeholder) {
    padding: 0 1em;
    color: #c0c4cc;
  }

  /* stylelint-disable-next-line selector-class-pattern */
  &:deep(.CodeMirror pre.CodeMirror-line, pre.CodeMirror-line-like) {
    padding: 0 1em;
  }
}

.editor-area {
  flex: 1;
  min-height: 100px;
  overflow: auto;

  :deep(.cm-editor .cm-content) {
    line-height: 2.3;
  }

  :deep(.cm-editor .cm-gutters) {
    line-height: 2.3;
  }
}

.primary-tip {
  margin-top: 0;
}
</style>
