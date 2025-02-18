<template>
  <div class="record-item">
    <pre v-highlight.noCopy @click="selectSQL"><code class="language-sql" v-text="sqlCode"></code></pre>
    <div class="btn">
      <el-icon :size="12" :title="t('common.copy')" @click="copy(sqlCode)"><DocumentCopy /></el-icon>
      <el-icon v-if="!props.isShared" :title="t('common.share')" @click="addSharedFavorite"><Share /></el-icon>
      <el-icon v-if="props.isCanDel" :size="12" :title="t('common.delete')" @click.stop="del"><Delete /></el-icon>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { copy } from 'utils';
import { getSqlProvider } from '../../../model/useExplorer';
import { ElMessageBox, ElMessage } from 'element-plus';
import { t } from 'locales';

interface Props {
  record: Recordable;
  isShared?: boolean;
  isCanDel?: boolean;
  requestApi: {
    del?: (id: string) => Promise<any>;
    addShared?: (sql: string) => Promise<any>;
  };
}
const props = withDefaults(defineProps<Props>(), {
  record: () => ({}),
  isShared: false,
  isCanDel: true
});
const { addSql } = getSqlProvider();
const requesting = ref(false);
const sqlCode = computed(() => props.record.sql);
const emits = defineEmits(['update']);

function selectSQL() {
  addSql('\n' + sqlCode.value, true);
}

async function del() {
  if (requesting.value) return;
  ElMessageBox.confirm(
    t('msg.confirmTemp', {
      operate: t('common.delete').toLocaleLowerCase(),
      name: sqlCode.value
    }),
    t('status.warning'),
    {
      confirmButtonText: t('common.confirm'),
      cancelButtonText: t('common.cancel'),
      type: 'warning'
    }
  )
    .then(() => {
      requesting.value = true;
      props.requestApi.del!(props.record.id)
        .then(() => {
          ElMessage.success(t('msg.deleteSuccess'));
          emits('update');
        })
        .finally(() => {
          requesting.value = false;
        });
    })
    .catch(() => {});
}
function addSharedFavorite() {
  if (requesting.value) return;
  ElMessageBox.confirm(
    t('msg.confirmTemp', {
      operate: t('common.share').toLocaleLowerCase(),
      name: sqlCode.value
    }),
    t('status.warning'),
    {
      confirmButtonText: t('common.confirm'),
      cancelButtonText: t('common.cancel'),
      type: 'warning'
    }
  )
    .then(() => {
      requesting.value = true;
      props.requestApi.addShared!(sqlCode.value)
        .then(() => {
          ElMessage.success(t('msg.addSuccess'));
          emits('update');
        })
        .finally(() => {
          requesting.value = false;
        });
    })
    .catch(() => {});
}
</script>

<style lang="scss" scoped>
$height: 30px;

.record-item {
  position: relative;
  display: flex;
  align-items: center;
  padding-left: 10px;
  font-family: Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace;
  font-size: 16px;
  line-height: $height;
  cursor: pointer;

  code {
    line-height: $height;
    white-space: normal;
  }

  .btn {
    position: absolute;
    right: 0;
    display: none;
    align-items: center;
    height: 100%;
    padding: 0 10px;
    font-size: 14px;
    color: rgb(25.8789% 34.8999% 80.7785%);
    background-color: #fff;

    & > i + i {
      margin-left: 10px;
      cursor: pointer;
    }
  }

  & + .record-item {
    margin-top: 10px;
  }
}

.record-item:hover {
  background-color: #efefef;

  .btn {
    display: flex;
  }
}
</style>
