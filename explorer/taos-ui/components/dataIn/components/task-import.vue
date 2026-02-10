<template>
  <el-upload
    class="upload-demo inline-upload"
    :action="dataInProps.uploadFileUrl"
    :data="{ req_id: 'taosx-demo-file' }"
    :on-success="handleSuccess"
    :on-progress="handleStart"
    :on-error="handleError"
    :file-list="fileList"
    :show-file-list="false"
  >
    <template v-if="$slots.btn"><slot name="btn"></slot></template>
    <el-button
      v-else
      v-loading.fullscreen.lock="requestIng"
      link
      type="primary"
      size="default"
      icon="SoldOut"
      :disabled="dataInProps.isCommunity"
      >{{ startCase(t('dataIn.import') + t('dataIn.task')) }}</el-button
    >
  </el-upload>

  <el-dialog v-model="dlgTaskListShow" :title="startCase(t('dataIn.import') + t('dataIn.task'))" width="800">
    <el-table :data="taskList" border style="width: 100%" @selection-change="handleSelectionChange">
      <el-table-column type="selection" width="55" />
      <el-table-column property="name" :label="t('dataIn.name2')" />
      <el-table-column :label="t('dataIn.type')" width="120">
        <template #default="scope">{{ scope.row.from.type }}</template>
      </el-table-column>
      <el-table-column :label="t('dataIn.via')" pro width="130">
        <template #default="scope">
          <el-select v-model="scope.row.via" :clearable="true" style="width: 100px; min-width: 100px">
            <el-option v-for="item in agentList" :key="`agent-${item.id}`" :label="item.name" :value="item.id">
            </el-option>
          </el-select>
        </template>
      </el-table-column>
      <el-table-column :label="t('dataIn.target')" pro width="210">
        <template #default="scope">
          <el-select v-model="scope.row.db" style="width: 180px; min-width: 180px">
            <el-option v-for="item in dbList" :key="`db-${item}`" :label="item" :value="item"> </el-option>
          </el-select>
        </template>
      </el-table-column>
    </el-table>
    <template #footer>
      <div class="dialog-footer">
        <el-button style="min-width: 100px" @click="dlgTaskListShow = false">{{ t('common.cancel') }}</el-button>
        <el-button style="min-width: 100px" type="primary" @click="importTasks">
          {{ t('common.confirm') }}
        </el-button>
      </div>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { t } from 'locales';
import { startCase } from 'lodash-es';
import { instance } from 'config';
import { getDataInProps } from '../../dataIn/model/useDataIn';
import { agentList } from '../../dataIn/model/util';

const dataInProps = getDataInProps();

const requestIng = ref(false);

const fileList = ref([]);
function handleStart() {
  requestIng.value = true;
}
function handleError() {
  requestIng.value = false;
}
function handleSuccess(_: any, file: { raw: Blob }) {
  const reader = new FileReader();

  reader.onload = e => {
    const contents = e.target?.result;
    requestIng.value = false;
    if (!contents) {
      ElMessage.error(t('dataIn.importEmpty'));
    } else {
      parseTaskFileContent(contents);
    }
  };

  reader.readAsText(file.raw); // 读取文本文件
}

const dlgTaskListShow = ref(false);
const taskList = ref([]);
const tasksToImport: any = {};

function parseTaskFileContent(contents: string) {
  let parsedContent;
  try {
    parsedContent = JSON.parse(contents);
  } catch (err) {
    ElMessage.error(err.message);
    return;
  }
  if (!parsedContent.tasks || parsedContent.tasks.length <= 0) {
    ElMessage.error(t('dataIn.importEmpty'));
    return;
  }
  parsedContent.tasks.forEach(task => {
    task.db = '';
    const lastIndex = task.to.lastIndexOf('/');
    if (lastIndex > 0) {
      const db = task.to.slice(lastIndex + 1);
      if (dbList.value.includes(db)) {
        task.db = db;
      }
    }
  });

  taskList.value = parsedContent.tasks;
  dlgTaskListShow.value = true;

  const keys = Object.getOwnPropertyNames(parsedContent);
  keys.forEach(key => {
    if (key !== 'tasks') {
      tasksToImport[key] = parsedContent[key];
    }
  });
}

const multipleSelection = ref<any[]>([]);
function handleSelectionChange(val: []) {
  multipleSelection.value = val;
}

const dbList = ref<any[]>([]);

const toUrl = computed(() => {
  const base_url = instance.gatewayUrl;
  const splitArr = base_url?.split('//') || [];
  const url = splitArr[0] + '//' + instance?.user + ':' + instance?.password + '@' + splitArr[1];
  return (splitArr[0].startsWith('taos') ? '' : 'taos+') + url + '/';
});
const emit = defineEmits(['importOK']);

async function importTasks() {
  const tasks = JSON.parse(JSON.stringify(multipleSelection.value));
  tasks.forEach((task: any) => {
    task.to = toUrl.value + task.db;
    delete task.db;
  });
  tasksToImport.tasks = tasks;
  tasksToImport.labels = [`cluster-id::${instance.tdClusterId}`, 'type::datain', `user::${instance?.user}`];
  requestIng.value = true;
  try {
    const res = await dataInProps.task.api.importTask(tasksToImport);
    dlgTaskListShow.value = false;
    if (res.code > 0 && res.message) {
      ElMessage.error(res.message);
    }
    emit('importOK');
  } catch (err) {
    ElMessage.error(err.message);
  } finally {
    requestIng.value = false;
  }
}

onMounted(async () => {
  const data = await dataInProps.dataSource.api.getDatabase();
  data.forEach(db => {
    if (db.name !== 'log' && db.name !== 'audit') {
      dbList.value.push(db.name);
    }
  });
});
</script>

<style scoped lang="scss">
/* 确保 upload 组件内的按钮样式与其他按钮一致 */
.inline-upload {
  display: inline-block;
  vertical-align: middle;
  position: relative;
  top: -2px; /* 向上移动 2 像素 */
}

:deep(.el-upload) {
  display: inline-block;
}

:deep(.el-button.is-link) {
  padding: 0 10px;
  margin: 0;
}
</style>
