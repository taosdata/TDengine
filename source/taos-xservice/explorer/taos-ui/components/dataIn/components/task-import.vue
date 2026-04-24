<template>
  <el-upload
    ref="uploadRef"
    v-bind="$attrs"
    class="upload-demo inline-upload hidden-upload"
    :before-upload="beforeUpload"
    :file-list="fileList"
    :show-file-list="false"
    accept=".json,.zip"
  >
    <span></span>
  </el-upload>
  <template v-if="$slots.btn">
    <span class="upload-trigger" @click="triggerImport">
      <slot name="btn"></slot>
    </span>
  </template>
  <template v-else>
    <el-button
      v-loading.fullscreen.lock="requestIng"
      link
      type="primary"
      size="default"
      icon="SoldOut"
      :disabled="dataInProps.isCommunity || dataInProps.xnodesExist === null"
      @click="triggerImport"
      >{{ startCase(t('dataIn.import') + t('dataIn.task')) }}</el-button
    >
  </template>

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
defineOptions({ inheritAttrs: false });
import { unzipSync } from 'fflate';
import { t } from 'locales';
import { startCase } from 'lodash-es';
import { instance } from 'config';
import { getDataInProps, uploadHeaders } from '../../dataIn/model/useDataIn';
import { agentList } from '../../dataIn/model/util';
import {
  bundledZipFileEntries,
  bundledZipUploadFileName,
  rewriteBundledReferencesInValue,
  singleUploadedPath
} from './taskImportFiles';

const dataInProps = getDataInProps();
const uploadRef = ref<{ $el: HTMLElement } | null>(null);

const requestIng = ref(false);
const fileList = ref([]);

/**
 * Intercept the upload before it reaches the server.
 * Both ZIP and JSON files are handled locally to avoid creating unused
 * upload buckets on the server during task import.
 */
async function beforeUpload(file: File): Promise<boolean | File | Blob> {
  requestIng.value = true;
  if (file.name.toLowerCase().endsWith('.zip')) {
    await handleZipImport(file);
    return false;
  }
  await handleJsonImport(file);
  return false;
}

async function handleJsonImport(file: File) {
  try {
    parseTaskFileContent(await file.text());
  } finally {
    requestIng.value = false;
  }
}

function importErrorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

/**
 * Handle a ZIP export file:
 * 1. Extract tasks.json and bundled config files using fflate.
 * 2. Re-upload each bundled file to /x/upload to obtain its new absolute path.
 * 3. Rewrite the relative "@files/…" references in tasks.json to the new paths.
 * 4. Hand off to the normal parseTaskFileContent flow.
 */
async function handleZipImport(blob: Blob | File) {
  try {
    const arrayBuffer = await blob.arrayBuffer();
    const uint8 = new Uint8Array(arrayBuffer);

    let zipFiles: Record<string, Uint8Array>;
    try {
      zipFiles = unzipSync(uint8);
    } catch (err) {
      ElMessage.error(t('dataIn.invalidZipFile', [(err as Error).message]));
      return;
    }

    const tasksJsonBytes = zipFiles['tasks.json'];
    if (!tasksJsonBytes) {
      ElMessage.error(t('dataIn.importEmpty'));
      return;
    }

    const tasksJsonText = new TextDecoder().decode(tasksJsonBytes);

    // Re-upload every bundled file and build an old-path → new-path mapping.
    const fileEntries = bundledZipFileEntries(zipFiles);
    if (fileEntries.length > 0) {
      const pathMap: Record<string, string> = {};
      try {
        for (const [zipPath, fileBytes] of fileEntries) {
          // zipPath = "files/{req_id}/{filename}"
          const afterPrefix = zipPath.slice('files/'.length); // "{req_id}/{filename}"
          const slashIdx = afterPrefix.indexOf('/');
          if (slashIdx === -1) continue;

          const fileName = bundledZipUploadFileName(zipPath);

          const formData = new FormData();
          // Always use a fresh upload bucket during import so we never overwrite
          // an existing file that happens to share the exported req_id/filename.
          formData.append('req_id', `import-${crypto.randomUUID()}`);
          formData.append('file', new Blob([fileBytes]), fileName);

          const response = await fetch(dataInProps.uploadFileUrl, {
            method: 'POST',
            credentials: 'include',
            // Include cloud auth headers when present (empty object is harmless).
            headers: uploadHeaders.value,
            body: formData,
          });

          if (!response.ok) {
            ElMessage.error(t('dataIn.failedToUpload', [fileName, response.statusText]));
            return;
          }

          const uploadedPaths: string[] = await response.json();
          const uploadedPath = singleUploadedPath(uploadedPaths, fileName);

          // JSON stores paths as "@files/{req_id}/{filename}" (relative).
          // Replace with "@{abs_path}" returned by the server.
          pathMap[`@${zipPath}`] = `@${uploadedPath}`;
        }
      } catch (err) {
        const message = err instanceof Error ? err.message : 'Unknown error';
        ElMessage.error(t('dataIn.zipImportUploadFailed', [message]));
        return;
      }

      try {
        const parsedTasksJson = JSON.parse(tasksJsonText);
        parseTaskFileContent(JSON.stringify(rewriteBundledReferencesInValue(parsedTasksJson, pathMap)));
        return;
      } catch (err) {
        ElMessage.error(importErrorMessage(err));
        return;
      }
    }

    parseTaskFileContent(tasksJsonText);
  } finally {
    requestIng.value = false;
  }
}

const dlgTaskListShow = ref(false);
const taskList = ref([]);
const tasksToImport: any = {};

function parseTaskFileContent(contents: string) {
  let parsedContent;
  try {
    parsedContent = JSON.parse(contents);
  } catch (err) {
    ElMessage.error(importErrorMessage(err));
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

function openFilePicker() {
  const fileInput = uploadRef.value?.$el?.querySelector('input[type="file"]') as HTMLInputElement | null;
  fileInput?.click();
}

async function triggerImport() {
  const { xnodesExist, missingXnodeCallback } = dataInProps;

  if (xnodesExist === null) {
    return;
  }

  if (xnodesExist === false) {
    // We already know no xnodes exist — show prompt without touching the file picker.
    if (missingXnodeCallback) {
      await missingXnodeCallback();
    }
    return;
  }

  // xnodesExist === true: open file picker synchronously
  // so that the browser's user-activation window is still valid.
  openFilePicker();
}

async function importTasks() {
  const tasks = JSON.parse(JSON.stringify(multipleSelection.value));
  tasks.forEach((task: any) => {
    task.to = toUrl.value + task.db;
    delete task.db;
  });
  tasksToImport.tasks = tasks;
  tasksToImport.labels = ['type::datain', `user::${instance?.user}`];
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

:deep(.hidden-upload) {
  width: 0;
  height: 0;
  overflow: hidden;
}

:deep(.el-upload) {
  display: inline-block;
}

.upload-trigger {
  display: inline-block;
}

:deep(.el-button.is-link) {
  padding: 0 10px;
  margin: 0;
}
</style>
