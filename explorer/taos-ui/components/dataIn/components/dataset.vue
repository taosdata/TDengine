<template>
  <div label-width="0px">
    <section class="flex-start mb20" :style="{ cursor: dataInProps.isCommunity ? 'not-allowed' : 'pointer' }">
      <el-tooltip placement="top" effect="light" :open-delay="0" :disabled="!dataInProps.isCommunity">
        <template #content>
          <span v-dompurify-html="$t('common.communityTip')"></span>
        </template>
        <el-button
          v-if="!isOpcDsnValid"
          size="default"
          plain
          type="primary"
          icon="Upload"
          :disabled="dataInProps.isCommunity"
          @click="handleBeforeUpload"
          >{{ t('dataIn.selectFile') }}</el-button
        >
      </el-tooltip>

      <UploadCsv
        ref="uploadCsvRef"
        v-model="fileValue"
        :config="config"
        :is-opc="isOpc"
        :is-opc-dsn-valid="isOpcDsnValid"
        @valid-opc-file="handleValidOpcFile"
      />

      <section v-if="isEdit" class="file-list">
        <div v-for="file in oldFiles" :key="file.name" class="file-item" @click="downloadByUrl(file.path, file.name)">
          <el-tooltip effect="light" :content="t('dataIn.downloadCSVInUseTip')">
            <a class="file-name">
              <el-icon><Download /></el-icon>
              <span>{{ t('dataIn.downloadCSVInUse') }}</span>
            </a>
          </el-tooltip>
        </div>
      </section>
    </section>
    <section class="mb20">
      <el-tooltip effect="light" :content="t('dataIn.downloadTemplateTip')">
        <a v-if="config.templateUrl" :class="{ disabled: dataInProps.isCommunity }" @click="handleDownEmptyTemplate">
          <el-icon><Download /></el-icon>
          {{ t('dataIn.downloadTemplate') }}</a
        >
      </el-tooltip>
      <el-tooltip class="opc-download-point" effect="light" :content="t('dataIn.downloadnodestip')">
        <a class="ml20" :class="{ disabled: dataInProps.isCommunity }" @click.prevent="openDialog">
          <el-icon><Download /></el-icon>
          {{ t('dataIn.downloadnodestip') }}
          <div class="csv-progress">
            <el-progress v-if="progressVisble" :percentage="percentage" :format="format" />
          </div>
        </a>
      </el-tooltip>
      <el-button v-if="isShowAddOpcPoint" type="primary" size="small" class="ml15" @click="handleOpcPoint">{{
        t('dataIn.addOpcPoint')
      }}</el-button>
      <el-button
        v-if="modelValue"
        :loading="loading"
        :disabled="loading"
        type="primary"
        size="small"
        class="ml15"
        @click="search"
        >{{ t('dataIn.transformer.preview') }}</el-button
      >
    </section>
    <el-dialog
      v-model="dialogVisible"
      :title="t('dataIn.filterPointTitle')"
      :close-on-click-modal="false"
      width="500px"
    >
      <template #header>
        <div>
          <div class="el-dialog-cus-title">{{ t('dataIn.filterPointTitle') }}</div>
          <DocsContent :content="t('dataIn.filterPoinDesc')" />
        </div>
      </template>
      <div>
        <el-form ref="conditionForm" size="default" :model="info" label-width="150px" label-position="left">
          <el-form-item :label="t('dataIn.rootNode')" prop="root">
            <el-input
              v-model="info.root"
              style="width: 300px"
              :placeholder="t('dataIn.rootNodePlaceholder.' + sourceForm.type)"
            ></el-input>
          </el-form-item>
          <el-form-item v-if="isOpcUa" :label="t('dataIn.namespace')" prop="namespaces">
            <el-select
              v-model="info.namespaces"
              style="width: 300px"
              :multiple="true"
              :placeholder="t('dataIn.namespacePlaceholder')"
            >
              <el-option
                v-for="item in namespaceList"
                :key="item.label"
                :value="item.value"
                :label="item.label"
              ></el-option>
            </el-select> </el-form-item
          ><el-form-item :label="t('dataIn.pointRegexp')" prop="pattern">
            <el-input
              v-model="info.pattern"
              style="width: 300px"
              :placeholder="t('dataIn.pointRegexpPlaceholder.' + sourceForm.type)"
            ></el-input>
          </el-form-item>
        </el-form>
      </div>
      <template #footer>
        <span class="dialog-footer">
          <el-button @click="dialogVisible = false">{{ t('common.cancel') }}</el-button>
          <el-button type="primary" :loading="requestIng" @click="submit">{{ t('common.confirm') }}</el-button>
        </span>
      </template>
    </el-dialog>
    <el-dialog
      v-model="dialogPointVisible"
      :title="t('dataIn.addOpcPoint')"
      :close-on-click-modal="false"
      width="600px"
    >
      <template #header>
        <div>
          <div class="el-dialog-cus-title">{{ t('dataIn.addOpcPoint') }}</div>
          <DocsContent :content="t('dataIn.addPointDesc')" />
        </div>
      </template>
      <div>
        <el-form ref="addPointFormRef" size="small" :model="opcPointForm" label-width="220px" label-position="left">
          <template v-for="(conf, index) in opcPointForm.opcCsvHeaders" :key="conf.name">
            <el-form-item
              :label="conf.is_tag ? `tag::${conf.type}::${conf.name}` : conf.name"
              :prop="'opcCsvHeaders.' + index + '.value'"
              :class="[{ 'hidden-required': !conf.required }]"
              :rules="[{ required: conf.required, message: t('common.requiredTemp', [conf.name]) }]"
            >
              <template #label>
                <el-tooltip v-if="conf.description" placement="top" effect="light" :open-delay="0">
                  <template #content>
                    <DocsContent v-if="conf.description" :content="isEn ? conf.description : conf.description_cn" />
                  </template>
                  <span>
                    <span>{{ conf.is_tag ? `tag::${conf.type}::${conf.name}` : conf.name }}</span>
                    <span v-if="config.description" style="margin-left: 1px">
                      <Icon name="label_info" class="info-icon-custom"></Icon>
                    </span>
                  </span>
                </el-tooltip>
              </template>
              <el-input v-if="!conf.choices" v-model="conf.value" style="width: 300px"></el-input>
              <el-select
                v-else
                v-model="conf.value"
                style="width: 300px"
                :placeholder="t('dataIn.namespacePlaceholder')"
              >
                <el-option v-for="item in conf.choices" :key="item" :value="item" :label="item"></el-option>
              </el-select>
            </el-form-item>
          </template>
        </el-form>
      </div>
      <template #footer>
        <span class="dialog-footer">
          <el-button @click="dialogPointVisible = false">{{ t('common.cancel') }}</el-button>
          <el-button type="primary" :loading="requestIng" @click="submitAddPoint">{{ t('common.confirm') }}</el-button>
        </span>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { FormInstance } from 'element-plus';
import UploadCsv from './uploadCsv.vue';
import DocsContent from 'components/MdRender.vue';
import { getDataInProps } from '../model/useDataIn';
import { downloadByData, downloadByUrl } from 'utils/files';
import { isEn } from 'config';
import useSearchPoint from '../model/useSearchPoint';
import { t } from 'locales';
import {
  currentPageType,
  sourceForm,
  connectivityCheckResult,
  validOpcFileResult,
  taskId,
  validateFormFields,
  formatFromData
} from '../model/util';

const dataInProps = getDataInProps();
const props = withDefaults(
  defineProps<{
    config: Record<string, any>;
    data: Record<string, any>;
    modelValue: string;
  }>(),
  {
    config: () => ({}),
    data: () => ({})
  }
);
const sourceParent = inject('sourceParent') as any;

const { loading, timer, search } = useSearchPoint();

interface InfoProps {
  root: string;
  namespaces: string[];
  pattern: string;
}
const localeData = reactive(props.data);
const requestIng = ref<boolean>(false);
const oldFiles = ref<any[]>([]);
const dialogVisible = ref<boolean>(false);
const progressVisble = ref<boolean>(false);
const info = reactive<InfoProps>({
  root: '',
  namespaces: [],
  pattern: ''
});
const ticket = ref('');
const percentage = ref(5);
const completed = ref<boolean>(false);
const dialogPointVisible = ref<boolean>(false);
const oldValue = ref<string>('');
const isOpcDsnValid = ref<boolean>(false); // 判断 opc 的 dsn 是否填了
const paramDsn = ref<string>('');
const uploadCsvRef = ref();
const addPointFormRef = ref<FormInstance>();
interface CsvHeadersItem {
  field: string;
  defaultValue: string;
  type: string;
  required: boolean;
  description: string;
  choices?: string[];
  is_tag?: boolean;
  name?: string;
  description_cn: string;
  value: string | number | [];
}
const opcPointForm: Recordable<CsvHeadersItem[]> = reactive({
  opcCsvHeaders: [
    {
      field: 'point_id',
      defaultValue: '',
      type: 'str',
      required: true,
      description: '数据点位在 OPC UA 服务器上的 id',
      description_cn: '',
      value: ''
    },
    {
      field: 'enable',
      defaultValue: '1',
      type: 'select',
      choices: ['1', '0'],
      required: false,
      description: '指定是否采集该点位数据。0-不采集并且删除对应子表，1-采集点位数据，没有子表时创建子表',
      description_cn: '',
      value: ''
    }
  ]
});

defineEmits(['update:modelValue']);

const fileValue = computed({
  get() {
    return localeData[props.config.field];
  },
  set(val) {
    localeData[props.config.field] = val;
  }
});
const allCategoryText = computed(() => t('dataIn.allNodes'));
const category = computed(() => 'nodes');
const isEdit = computed(() => currentPageType.value == 'edit');
const isOpc = computed(() => ['opcua', 'opcda'].includes(sourceForm.type));
const isOpcUa = computed(() => ['opcua'].includes(sourceForm.type));
const agentId = computed(() => sourceForm.agent);
const namespaceList = computed(() => {
  const { namespaces = [] } = connectivityCheckResult.value;
  const list: Recordable[] = [];
  namespaces.map((item, index) => {
    if (index > 0) {
      list.push({ label: item, value: index });
    }
  });
  return list;
});

const isShowAddOpcPoint = computed(() => {
  // 重新上传了一个 csv,此时的任务还没有提交，因此csv没有生效，所有也不应该显示增加点位按钮
  return oldValue.value && props.modelValue != '*' && props.modelValue === oldValue.value && isEdit.value;
});

watch(completed, val => {
  if (val) {
    timer.value && clearInterval(timer.value);
    percentage.value = 100;
    // 调用下载接口
    downloadFile();
  }
});

onMounted(() => {
  if (props.modelValue != '*' && props.modelValue && isEdit.value) {
    oldFiles.value = getFileList(props.modelValue);
    oldValue.value = props.modelValue;
  }
});

function getFileList(data: string) {
  return data.split(',').map(item => {
    const name = item.slice(item.lastIndexOf('/') + 1);
    const path = item.slice(1);
    return {
      name,
      path
    };
  });
}
function openDialog() {
  validateFormFields(sourceParent?.refs.formRef, () => {
    dialogVisible.value = true;
  });
}
// 下载数据点位准备阶段
async function submit() {
  if (requestIng.value) return;
  try {
    requestIng.value = true;

    const via = sourceForm.agent;
    const params: Recordable = {
      from_json: { ...sourceForm, ...info },
      categories: category.value
    };

    if (via) {
      params.via = via;
    }

    progressVisble.value = true;
    const result = await dataInProps.dataSource.api.fechTicketApi(params);
    ticket.value = result.ticket;

    timer.value = setInterval(async () => {
      const { complete } = await dataInProps.dataSource.api.checkReadyFile(result.ticket);
      completed.value = complete;
      const randomNum = Math.floor(Math.random() * 4);

      if (!complete) {
        percentage.value = percentage.value < 95 ? percentage.value + randomNum : 99;
      }
    }, 2000);
    dialogVisible.value = false;
  } catch (error) {
    timer.value && clearInterval(timer.value);
  }
}
// 下载 OPC 数据点位
async function downloadFile() {
  const res = await dataInProps.dataSource.api.fechOpcPointFileApi(ticket.value);
  if (res && res.code) {
    return ElMessage.error(res.message);
  }
  downloadByData(res as BlobPart, allCategoryText.value + '.csv');
  completed.value = false;
  requestIng.value = false;
  setTimeout(() => {
    progressVisble.value = false;
    percentage.value = 5;
  }, 500);
}
function format(percentage: number) {
  return `${percentage}%`;
}
// 下载 CSV 空模版
async function handleDownEmptyTemplate() {
  const res = await dataInProps.dataSource.api.fechCsvEmptyTemplateApi(sourceForm.type);
  downloadByData(res, t('dataIn.downloadTemplate') + '.csv');
}
async function handleOpcPoint() {
  // 获取csv header
  const result = await dataInProps.dataSource.api.fechOpcCsvHeaderApi(taskId.value);
  if (result && Object.hasOwnProperty.call(result, 'code')) {
    ElMessage.error(result?.message);
    return;
  }
  dialogPointVisible.value = true;
  opcPointForm.opcCsvHeaders = result.map((item: any) => {
    item.value = item.defaultValue;
    return item;
  });
}
function submitAddPoint() {
  if (requestIng.value) return;
  addPointFormRef.value?.validate(async valid => {
    if (!valid) return;
    const params: any = {
      point: opcPointForm.opcCsvHeaders,
      task_id: taskId.value
    };
    if (agentId.value) {
      params.via = agentId.value;
    }
    const result = await dataInProps.dataSource.api.addOpcPointApi(params);
    if (result && Object.hasOwnProperty.call(result, 'code')) {
      requestIng.value = false;
      ElMessage.error(result?.message);
      return;
    }
    ElMessage.success({
      message: t('dataIn.addPointSucc'),
      duration: 30000,
      showClose: true
    });
    requestIng.value = false;
    opcPointForm.opcCsvHeaders.map(item => {
      if (item.name == 'point_id' || item.name == 'tag_name') {
        item.value = '';
      }
    });
  });
}

function onValid() {
  isOpcDsnValid.value = true;
  nextTick(() => {
    const uploadButton = uploadCsvRef.value?.$refs?.uploadButtonRef;
    if (uploadButton) {
      uploadButton.$el.click(); // 手动触发 el-button 的点击事件
    }
    isOpcDsnValid.value = false;
  });
}

function onInvalid() {
  isOpcDsnValid.value = false;
  paramDsn.value = '';
}

// opc dataset 上传 csv 文件需要需要做合法性检查，合法性检查的参数需要dsn，所以需要必填项检查的校验
function handleBeforeUpload() {
  validateFormFields(sourceParent?.refs.formRef, onValid, onInvalid);
}

async function handleValidOpcFile() {
  // csv 文件合法性检查
  const params = {
    dsn: formatFromData(sourceForm)
  };

  const result = await dataInProps.dataSource.api.validOpcFile(params);
  // eslint-disable-next-line no-prototype-builtins
  if (result && result.hasOwnProperty('code')) {
    ElMessage.error(result.message);
    const res = {
      valid: false,
      message: result.message
    };
    // 全局的参数用于提交的时候再次判断
    validOpcFileResult.value = res;
  } else {
    validOpcFileResult.value = result;
    ElMessage.success(result.message);
  }
  isOpcDsnValid.value = false;
}

onBeforeUnmount(() => {
  timer.value && clearInterval(timer.value);
});
</script>

<style scoped lang="scss">
$color-primary: rgb(25 34 80);

.file-list {
  margin-left: 20px;
  color: $color-primary;

  .file-item {
    display: flex;
    align-items: center;
    font-size: 14px;
    cursor: pointer;

    .file-name {
      flex: 1;

      @extend .no-wrap !optional; // 样式可能不生效

      & > i {
        margin-right: 3px;
      }
    }

    .file-btn {
      display: none;
      padding-left: 20px;
      font-size: 12px;

      span {
        cursor: pointer;

        & + span {
          margin-left: 10px;
        }
      }
    }

    &:hover {
      color: $color-primary;
      background-color: #f5f7fa;

      .file-btn {
        display: flex;
      }
    }
  }
}

.opc-download-point {
  position: relative;
}

.csv-progress {
  position: absolute;
  width: 150px;

  // left: 18px;
}

.el-dialog-cus-title {
  font-size: 20px;
  font-weight: 500;
  line-height: 26px;
  color: #4d6992;
}

.disabled {
  pointer-events: none;
  filter: alpha(opacity=50);
  -moz-opacity: 0.5;
  opacity: 0.5;
}

:deep(.el-form-item) {
  margin-bottom: 18px;
}
</style>
