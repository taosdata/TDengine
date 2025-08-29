<template>
  <div class="page-wrapper">
    <!-- <PageHeader :title="props.pageTitle"></PageHeader> -->
    <div class="content">
      <Left id="left"></Left>
      <div id="drag-bar"></div>
      <Right>
        <template #detail>
          <slot name="detail"></slot>
        </template>
      </Right>
    </div>
  </div>
</template>

<script lang="ts" setup>
import Left from './components/left.vue';
import Right from './components/right.vue';
import { ExplorerProps, explorerPropsKey, sqlProviderKey, setCustomCompCallback } from './model/useExplorer';
import {
  handleSqlExecuteSuccess,
  handleSqlExecuteFail,
  InfoData,
  currentInfoDataProviderKey,
  backSqlPart
} from './components/utils';
import { ElMessage, ElMessageBox } from 'element-plus';
import { executeSqlFn } from '../api';

const props = defineProps<ExplorerProps>();
const sqlStr = ref('');
const addSql = (sql: string, append = false) => {
  if (append) {
    sql = sqlStr.value + sql;
  }
  sqlStr.value = sql;
};
const sqlExecuting = ref(false);
provide('treeKey', ref(0));

provide(sqlProviderKey, {
  sqlStr,
  addSql,
  executeSql,
  sqlExecuting
});
provide(explorerPropsKey, props);
setCustomCompCallback(props.customCompCallback);

// 切换实例时初始化已保存的变量
onMounted(() => {
  dragChangeWidth();
  backSqlPart(true);
});
const currentInfoData: InfoData = reactive({
  db: {},
  stb: {},
  tb: {},
  type: 'db'
});

provide(currentInfoDataProviderKey, currentInfoData);

function executeSql(sql = sqlStr.value) {
  if (sqlExecuting.value) return;
  if (!executeSqlFn) return ElMessageBox.alert('请先设置请求函数');
  sqlExecuting.value = true;
  const startTime = Date.now();
  executeSqlFn(sql, false)
    .then(data => {
      handleSqlExecuteSuccess(data, sqlStr.value, startTime);
    })
    .catch(data => {
      ElMessage.closeAll();
      if (data) {
        ElMessage.error(data);
      }
      handleSqlExecuteFail(data, sqlStr.value, startTime);
    })
    .finally(() => {
      sqlExecuting.value = false;
    });
}
function dragChangeWidth() {
  const dragEl = document.getElementById('drag-bar');
  const panelEl = document.getElementById('left');
  if (!dragEl || !panelEl) return;
  dragEl.onmousedown = ev => {
    const disW = panelEl.offsetWidth;
    const disX = ev.clientX;

    document.onmousemove = ev => {
      panelEl.style.width = disW + (ev.clientX - disX) + 'px';
    };
    document.onmouseup = () => {
      document.onmousemove = document.onmouseup = null;
    };
    return false;
  };
}
</script>

<style lang="scss" scoped>
$drag-bar-color: #f5f5f5;
$drag-bar-light-color: #dcdfe6;

.content {
  flex-direction: row;
  width: 100%;
  height: 100%;
  padding: 0;
  overflow: hidden;
}

#drag-bar {
  width: 10px;
  height: 100%;
  cursor: e-resize;
  background-color: $drag-bar-color;
}

#drag-bar:hover {
  background-color: $drag-bar-light-color;
}
</style>
