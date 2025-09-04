<template>
  <div class="source-ui">
    <div :class="['left-ui']">
      <el-form
        ref="formRef"
        :model="sourceForm"
        label-width="260px"
        label-position="left"
        size="default"
        :rules="rules"
      >
        <section class="block-wrapper">
          <el-form-item :label="t('dataIn.name2')" prop="name">
            <el-input id="name" v-model="sourceForm.name" :placeholder="t('dataIn.placeholders.taskName')"></el-input>
          </el-form-item>
          <el-form-item :label="t('dataIn.type')" prop="type" class="hidden-required">
            <el-select id="type" v-model="sourceForm.type" :disabled="!!taskId" @change="typeChang">
              <el-option
                v-for="item in definitionsList"
                :key="item.name"
                :label="item.name"
                :value="item.id"
              ></el-option>
            </el-select>
          </el-form-item>
          <el-form-item v-if="isShowAgent" prop="agent" class="hidden-required">
            <template #label>
              <el-tooltip placement="top" effect="light">
                <template #content>
                  <div v-dompurify-html="t('dataIn.needAgentTip')"></div>
                </template>
                <div>
                  <span>{{ t('dataIn.agent') }}</span>
                  <span style="margin-left: 1px">
                    <Icon name="label_info" class="info-icon-custom"></Icon>
                  </span>
                </div>
              </el-tooltip>
            </template>
            <el-select
              id="agent"
              v-model="sourceForm.agent"
              style="width: 190px"
              :placeholder="t('dataIn.placeholders.agentPlaceholder')"
              clearable
            >
              <el-option v-for="item in agentList" :key="item.name" :label="item.name" :value="item.id"></el-option>
            </el-select>
            <el-tooltip placement="top" effect="light" :open-delay="0" :disabled="!dataInProps.isCommunity">
              <template #content>
                <span v-dompurify-html="t('common.communityTip')"></span>
              </template>
              <el-button
                :disabled="dataInProps.isCommunity"
                type="primary"
                plain
                class="ml15"
                icon="Plus"
                @click="createAgent"
                >{{ t('dataIn.createNewAgent') }}</el-button
              >
            </el-tooltip>
          </el-form-item>
          <el-form-item :label="t('dataIn.target')" prop="targetDB">
            <el-select
              id="targetDB"
              v-model="sourceForm.targetDB"
              :placeholder="t('dataIn.placeholders.chooseTargetDbTip')"
              style="width: 190px"
              @change="targetDBChange"
            >
              <el-option v-for="item in databaseList" :key="item.name" :value="item.name"></el-option>
            </el-select>
            <el-tooltip placement="top" effect="light" :open-delay="0" :disabled="!dataInProps.isCommunity">
              <template #content>
                <span v-dompurify-html="t('common.communityTip')"></span>
              </template>
              <el-button
                :disabled="dataInProps.isCommunity"
                type="primary"
                plain
                class="ml15"
                icon="Plus"
                @click="createDatabase"
                >{{ t('dataIn.createDatabase') }}</el-button
              >
            </el-tooltip>
          </el-form-item>
        </section>
        <ConfigForm
          v-if="currentDefinition && currentDefinition.config && sourceForm.data"
          ref="configformRef"
          :key="componentKey"
          :config="currentDefinition.config"
          :data="sourceForm.data"
          :parser="currentDefinition.parser"
          parent="data."
          :level="1"
        />
      </el-form>

      <section class="bottom">
        <el-affix position="bottom" offset="0">
          <div class="btn-group-task">
            <el-tooltip placement="top" effect="light" :open-delay="0" :disabled="!dataInProps.isCommunity">
              <template #content>
                <span v-dompurify-html="t('common.communityTip')"></span>
              </template>
              <el-button
                type="primary"
                size="default"
                :loading="loading"
                :disabled="dataInProps.isCommunity"
                @click="save"
              >
                {{ currentPageType === 'edit' ? t('dataIn.saveAndApply') : t('dataIn.submit') }}
              </el-button>
            </el-tooltip>
            <el-button class="cancel-btn" size="default" @click="goTaskPage">{{ t('common.cancel') }}</el-button>
          </div>
        </el-affix>
      </section>
    </div>

    <div class="right-ui">
      <div class="doc-part">
        <DocsContent
          v-if="currentDefinition?.description"
          class="mt20"
          :content="currentDefinition.description"
        ></DocsContent>
      </div>
      <ResultTable
        v-if="transformerState.showResultTb"
        :is-editable="currentPageType === 'edit' || currentPageType === 'copy'"
        :current-data-source="sourceForm.type"
      ></ResultTable>
      <DatasetTable v-if="isShowDatasetTable" />
    </div>
    <CreateDatabaseDialog
      v-model="dialogVisible"
      :db-list="databaseList"
      :create-api="dataInProps.dataSource.api.createApi"
      @close="close"
      @update="handleDatabaseUpdate"
    />
    <CreateAgentDialog
      v-model="dialogAgentVisible"
      :agent-list="agentList"
      @close="closeDialogAgent"
      @update="handleAgentUpdate"
    />
  </div>
</template>
<script setup lang="ts">
import { ElMessage, ElMessageBox, FormInstance } from 'element-plus';
import { isEqualWith, isEqual, isArray, cloneDeep } from 'lodash-es';
import { useRoute, useRouter } from 'hooks/useCurrentRouter';
import {
  getSourceConfig,
  getAgentList,
  agentList,
  NoNeedAgentType,
  currentPageType,
  generateFormInitData,
  currentDefinition,
  sourceForm,
  taskId,
  isShowDatasetTable,
  currentTaskStatus,
  connectivityCheckResult,
  validOpcFileResult,
  getAdvancedHealth,
  formatFromData,
  recoverFromData,
  recoverWriteConfig
} from '../model/util';
import { transformerState, configureSupportFlags, resetTransformerState } from '../components/commonTransformer/util';
import { getDataInProps } from '../model/useDataIn';
import CreateDatabaseDialog from '../components/addDbDialog.vue';
import CreateAgentDialog from '../components/addAgentDialog.vue';
import DatasetTable from '../components/datasetTablePreview.vue';
import ConfigForm from '../components/configForm.vue';
import ResultTable from '../components/commonTransformer/transformResultTable.vue';
import { isEn } from 'config';
import { t } from 'locales';
import { TransformerfullparamsType } from '../components/commonTransformer/type';
import DocsContent from 'components/MdRender.vue';
import { instance } from 'config';

const dataInProps = getDataInProps();
provide('sourceParent', getCurrentInstance());
provide('getCurrentDefinition', () => currentDefinition);

const route = useRoute();
const router = useRouter();
// 当前页面的类型
const databaseList = ref<Recordable[]>([]);
const dialogVisible = ref<boolean>(false);
const dialogAgentVisible = ref<boolean>(false);
const loading = ref<boolean>(false);
const formRef = ref<FormInstance>();
const configformRef = ref();
const componentKey = ref(0);
const oldParams = reactive({});
const definitionsList: Recordable = ref([]);
let defaultConfig = reactive<Recordable>({});

const isAddable = computed(() => currentPageType.value === 'add');
const isEditable = computed(() => currentPageType.value === 'edit');
const isCopyable = computed(() => currentPageType.value === 'copy');

const rules = computed(() => {
  return {
    name: [
      {
        required: true,
        trigger: 'blur',
        message: t('common.requiredTemp', [t('dataIn.name2')])
      }
    ],
    targetDB: {
      required: true,
      trigger: 'change',
      message: t('common.requiredTemp', [t('dataIn.target')])
    }
  };
});

const toUrl = computed(() => {
  if (dataInProps.isCloud) {
    return (
      'taos+' + instance?.gatewayUrl.replace('http', 'ws') + '/' + sourceForm.targetDB + '?token=' + instance?.token
    );
  }

  const base_url = instance.gatewayUrl;
  const splitArr = base_url?.split('//') || [];
  const url = splitArr[0] + '//' + instance?.user + ':' + instance?.password + '@' + splitArr[1];
  return (splitArr[0].startsWith('taos') ? '' : 'taos+') + url + (sourceForm.targetDB ? '/' + sourceForm.targetDB : '');
});

const labels = computed(() => {
  if (dataInProps.isCloud) {
    return ['ds', sourceForm.type, 'name::' + sourceForm.name, 'dsType::' + sourceForm.type];
  }
  return ['type::datain', `cluster-id::${instance?.tdClusterId}`, `user::${instance?.user}`];
});

provide('toUrl', toUrl);

taskId.value = Number(route?.params.taskId);

watch(
  isEn,
  val => {
    defaultConfig = getSourceConfig(val);
    getDataSource();
  },
  {
    immediate: true
  }
);

if (route?.params.page === 'edit' || route?.params.page === 'copy') {
  currentPageType.value = route?.params.page;
} else {
  currentPageType.value = 'add';
  sourceForm.name = '';
  sourceForm.targetDB = '';
}

onMounted(async () => {
  if (route?.params.page === 'edit' || route?.params.page === 'copy') {
    await handleDetailData(route?.params.taskId);
  } else {
    dataInProps.isIndusty ? (sourceForm.type = 'csv') : (sourceForm.type = 'tmq');
    getDataSource();
  }
});

const isShowAgent = computed(() => !NoNeedAgentType.includes(sourceForm.type));

async function handleDetailData(id: string | number) {
  const data = await dataInProps.dataSource.api.getTaskDetailApi(id);
  if (data.from_json) {
    data.from = data.from_json;
  }

  sourceForm.agent = data.via;
  sourceForm.type = data.from.type;
  getDataSource();

  sourceForm.name = data.name;
  sourceForm.targetDB = data.to_expand.subject;
  // sourceForm.data = data.from.data;

  if (sourceForm.type == 'csv') {
    transformerState.csvParser = data.parser;

    sourceForm.data.csvData = {
      currentTab: 'upload_csv_file',
      path: '',
      monitor_file_directory: {
        file_url: '',
        file_pattern: '',
        new_file_notify: false,
        notify_interval: 30,
        sort: '1'
      },
      upload_csv_file: {
        keep_processed_files: false,
        file_url: ''
      }
    };
  }

  recoverFromData(sourceForm.type, sourceForm.data, data.from.data);
  if (data.parser?.parser?.global) {
    recoverWriteConfig(sourceForm.data.write_config, data.parser.parser.global);
  }

  if (data.parser) {
    transformerState.transformerParserData = data.parser;
  }
}

function getDataSource() {
  currentDefinition.value = defaultConfig.defaultSourceConfig[sourceForm.type];
  definitionsList.value = defaultConfig.definitionsList;
  if (!currentDefinition.value) return;
  sourceForm.data = generateFormInitData(currentDefinition.value?.config);
  configureSupportFlags(sourceForm.type);
  componentKey.value++;
}
async function getDatabaseList() {
  try {
    const data = await dataInProps.dataSource.api.getDatabase();
    databaseList.value = data.filter(v => v.name !== 'audit' && v.name !== 'log');

    // 在编辑状态下，判断如果 targetDb 不为空，并且 targetDB 不在 dbList 中，则将 targetDB 置空
    if (taskId.value) {
      clearTargetDBWhenDelete();
    }
  } catch (error) {
    console.log(error);
  }
}
getDatabaseList();
function clearTargetDBWhenDelete() {
  if (sourceForm.targetDB && !databaseList.value.find(v => v.name === sourceForm.targetDB)) {
    sourceForm.targetDB = '';
  }
}
function typeChang() {
  defaultConfig = getSourceConfig(isEn.value);
  currentDefinition.value = defaultConfig.defaultSourceConfig[sourceForm.type];
  definitionsList.value = defaultConfig.definitionsList;

  if (!currentDefinition.value) return;
  sourceForm.agent = '';
  sourceForm.data = generateFormInitData(currentDefinition.value?.config);
  componentKey.value++;
  configureSupportFlags(sourceForm.type);
  isShowDatasetTable.value = false;
}
function targetDBChange() {
  // 在任何状态下目标数据库改变清空超级表和 mapping table
}
function createDatabase() {
  dialogVisible.value = true;
}
function close() {
  dialogVisible.value = false;
}
function handleDatabaseUpdate(name: string) {
  sourceForm.targetDB = name;
  getDatabaseList();
  close();
}
function handleAgentUpdate(name: string) {
  sourceForm.agent = name;
  getAgentList(dataInProps.agent.api);
}
getAgentList(dataInProps.agent.api);
function createAgent() {
  dialogAgentVisible.value = true;
}
function closeDialogAgent() {
  dialogAgentVisible.value = false;
}
function save() {
  loading.value = true;
  const status = currentTaskStatus.value;
  if (
    currentPageType.value === 'edit' &&
    // !currentPageType.value === 'copy' &&
    !['stopped', 'completed'].includes(status)
  ) {
    ElMessageBox.confirm(t('dataIn.saveTip'), t('common.warning'), {
      confirmButtonText: t('common.confirm'),
      cancelButtonText: t('common.cancel'),
      type: 'warning'
    })
      .then(() => {
        submit();
      })
      .catch(() => {
        loading.value = false;
      });
  } else {
    submit();
  }
}
function isEqualParams(obj1: any, obj2: any) {
  return isEqualWith(obj1, obj2, (item1, item2) => {
    if (isArray(item1) && isArray(item2)) {
      return isEqual(item1.sort(), item2.sort());
    }
  });
}

interface paramsProps {
  from?: string;
  from_json: Recordable;
  name: string;
  to: string;
  labels: string[];
  via?: number;
  parser?: TransformerfullparamsType;
  to_cluster?: string;
  trigger?: Recordable;
}
async function submit() {
  formRef.value?.validate(async (valid: boolean) => {
    if (valid) {
      const type = sourceForm.type;
      const params = {
        from: '',
        from_json: formatFromData(sourceForm),
        name: sourceForm.name,
        to: toUrl.value,
        labels: labels.value
      } as paramsProps;

      const health = getAdvancedHealth(sourceForm.data['advanced_options']);
      if (health) {
        params.trigger = { health };
      }

      if (isAddable && dataInProps.isCloud) {
        params.to_cluster = instance.tdClusterId;
      }
      if (type == 'csv') {
        const flag = configformRef.value?.$refs.csvDataRef[0].submitParse();
        if (!flag) {
          loading.value = false;
          return;
        }
        await configformRef.value?.$refs.csvDataRef[0].$refs.transformRef.getTransformerParams();

        if (configformRef.value?.$refs.csvDataRef[0].$refs.transformRef.isbreak) {
          loading.value = false;
          return;
        }

        params.parser = transformerState.transformerfullparams as TransformerfullparamsType;
      }

      if (type.startsWith('opc') && sourceForm.data.datasets?.csv_config_file && !validOpcFileResult.value?.valid) {
        loading.value = false;
        ElMessage.error(validOpcFileResult.value?.message);
        return;
      }
      if (type !== 'csv') {
        await configformRef.value?.$refs.checkConnectivityRef[0].clickCheckBtn();
        const { valid, support } = connectivityCheckResult.value;
        if (!valid || !support) {
          loading.value = false;
          return;
        }
      }
      if (type == 'pibackfill') {
        const backfillEndTime = sourceForm.data.groups_after.backfill.BackfillEndTime;
        if (backfillEndTime) {
          const backfillEndTimeValue = new Date(backfillEndTime).getTime();
          const currentTime = new Date().getTime();
          if (backfillEndTimeValue > currentTime) {
            ElMessage.error(t('dataIn.backfillEndTimeTip'));
            loading.value = false;
            return;
          }
        }
      }

      if (sourceForm.agent) {
        params['via'] = Number(sourceForm.agent);
      }

      if (sourceForm.data.parser) {
        await configformRef.value?.$refs.transformRef[0].getTransformerParams();
        if (configformRef.value?.$refs.transformRef[0].isbreak) {
          loading.value = false;
          return;
        }
        params.parser = transformerState.transformerfullparams as TransformerfullparamsType;
      }

      if (isEditable.value && taskId.value && !isCopyable.value) {
        const newParams: Recordable = cloneDeep(params);
        if (newParams.from) {
          delete newParams.from;
        }
        newParams.data = sourceForm.data;

        if (type == 'csv' || !isEqualParams(oldParams, newParams)) {
          const result = await dataInProps.dataSource.api.editSourceApi(params, taskId.value);
          loading.value = false;
          if (result.message) {
            ElMessage.error(result.message);
            return;
          }
        }
        formRef.value?.resetFields();
        goTaskPage();
      } else {
        const result = await dataInProps.dataSource.api.addSourceApi(params);
        loading.value = false;
        if (result.message) {
          ElMessage.error(result.message);
          return;
        }
        formRef.value?.resetFields();
        goTaskPage();
      }
    } else {
      console.log('sourceForm.-submit:', sourceForm);
      nextTick(() => {
        document.querySelector('.source-ui .left-ui .is-error')?.scrollIntoView();
      });
      loading.value = false;
    }
  });
}
function goTaskPage() {
  router.push({
    path: '/dataIn/Task'
  });
}

onBeforeUnmount(() => {
  resetTransformerState();
});
</script>
<style lang="scss" scoped>
$color-description: rgb(137 130 130);

.source-ui {
  display: flex;
  justify-content: space-between;
  overflow-x: auto;

  .left-ui.readable {
    position: relative;

    &::before {
      position: absolute;
      inset: 0;
      z-index: 100;
      display: block;
      content: '';
      background: #f2f6fc40;
    }
  }

  .left-ui {
    flex-shrink: 0;
    width: 50%;
    min-width: 800px;
    margin-top: 10px;

    .description {
      max-width: 568px;
      overflow: auto;
    }

    section {
      padding: 15px;
      margin-bottom: 20px;
      border: 1px solid #ececef;
      border-radius: 12px;

      // border-bottom: 1px solid #ececef;
    }

    .bottom {
      padding: 0 !important;
      margin-bottom: 0;
      border: none !important;

      .btn-group-task {
        display: flex;
        padding: 20px 15px;
        background: #fafafa;

        .el-button {
          flex: 1;
        }

        .el-select {
          margin-left: 0 !important;
        }
      }
    }

    :deep(.el-input-number__increase),
    :deep(.el-input-number__decrease) {
      display: flex;
      align-items: center;
      justify-content: center;
      height: 30px;
    }
  }

  .right-ui {
    position: relative;
    flex: 1;
    margin-left: 40px;
    overflow: hidden;

    .doc-part {
      padding: 2rem;
      margin: 10px 1rem;
      background: rgb(251 251 251);
      border-radius: 0.8rem;
      box-shadow: rgb(0 0 0 / 10%) 0 0 15px;
    }

    &:deep(.markdown-body) {
      background: rgb(251 251 251);

      & ul,
      ol {
        padding-left: 0;
      }
    }
  }

  .preview-btn,
  .cancel-btn,
  .edit-btn,
  .upload-flex .item {
    z-index: 101;
  }

  .custom-placeholder {
    margin-top: 10px;
    font-size: 14px;
    color: $color-description;
  }
}
</style>
<style lang="scss">
.hidden-required {
  .el-form-item__label {
    display: flex;

    &::before {
      margin-right: 4px;
      color: red;
      visibility: hidden;
      content: '*';
    }
  }
}
</style>
