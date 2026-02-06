<template>
  <div label-width="0px">
    <section class="flex-start mb-20px" :style="{ cursor: dataInProps.isCommunity ? 'not-allowed' : 'pointer' }">
      <el-tooltip placement="top" effect="light" :open-delay="0" :disabled="!dataInProps.isCommunity">
        <template #content>
          <span v-dompurify-html="$t('common.communityTip')"></span>
        </template>
        <el-button
          v-if="isOpc && !isOpcDsnValid"
          size="default"
          plain
          :type="dataInProps.isIdmp ? 'default' : 'primary'"
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
        <div v-for="file in oldFiles" :key="file.name" class="file-item" @click="handleDownloadOldFile(file)">
          <el-tooltip effect="light" :content="t('dataIn.downloadCSVInUseTip')">
            <a class="file-name">
              <el-icon><Download /></el-icon>
              <span>{{ t('dataIn.downloadCSVInUse') }}</span>
            </a>
          </el-tooltip>
        </div>
      </section>
    </section>
    <section class="mb-20px">
      <el-tooltip effect="light" :content="t('dataIn.downloadTemplateTip')">
        <a v-if="config.templateUrl" :class="{ disabled: dataInProps.isCommunity }" @click="handleDownEmptyTemplate">
          <el-icon><Download /></el-icon>
          {{ t('dataIn.downloadTemplate') }}</a
        >
      </el-tooltip>
      <el-tooltip v-if="isOpc" class="opc-download-point" effect="light" :content="t('dataIn.downloadnodestip')">
        <a class="ml20" :class="{ disabled: dataInProps.isCommunity }" @click.prevent="openDialog">
          <el-icon><Download /></el-icon>
          {{ t('dataIn.downloadnodestip') }}
          <div class="csv-progress">
            <el-progress v-if="progressVisible" :percentage="percentage" :format="format" />
          </div>
        </a>
      </el-tooltip>
      <el-tooltip v-if="isKinghist" class="opc-download-point" effect="light" :content="t('dataIn.downloadnodestip')">
        <a class="ml20" :class="{ disabled: dataInProps.isCommunity }" @click.prevent="openKingDialog">
          <el-icon><Download /></el-icon>
          {{ t('dataIn.downloadnodestip') }}
          <div class="csv-progress">
            <el-progress v-if="progressVisible" :percentage="percentage" :format="format" />
          </div>
        </a>
      </el-tooltip>
      <el-button
        v-if="isShowAddOpcPoint"
        :type="dataInProps.isIdmp ? 'default' : 'primary'"
        size="small"
        class="ml15"
        @click="handleOpcPoint"
        >{{ t('dataIn.addOpcPoint') }}</el-button
      >
      <el-button
        v-if="isOpc && modelValue"
        :loading="loading"
        :disabled="loading"
        :type="dataInProps.isIdmp ? 'default' : 'primary'"
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
          <DocsContent :content="t('dataIn.filterPointDesc')" />
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
    <!-- KingHistorian Download Points Dialog -->
    <el-dialog
      v-model="kingDialogVisible"
      :title="t('dataIn.filterPointTitle')"
      :close-on-click-modal="false"
      width="520px"
    >
      <template #header>
        <div>
          <div class="el-dialog-cus-title">{{ t('dataIn.filterPointTitle') }}</div>
          <DocsContent :content="t('dataIn.filterPointDesc')" />
        </div>
      </template>
      <div>
        <el-form ref="kingFormRef" size="default" :model="kingFilters" label-width="150px" label-position="left">
          <el-form-item :label="t('dataIn.kinghist.group')" prop="group">
            <el-select
              ref="groupSelectRef"
              v-model="kingFilters.group"
              filterable
              :loading="kingLoading.groups"
              style="width: 320px"
              clearable
            >
              <el-option v-for="item in groupOptions" :key="item.id" :label="item.label" :value="item.id" />
              <template #empty>
                <div v-if="kingLoading.groups" class="select-empty">{{ t('common.loading') }}</div>
                <div v-else class="select-empty">{{ t('common.noData') }}</div>
              </template>
            </el-select>
          </el-form-item>
          <el-form-item :label="t('dataIn.kinghist.point')" prop="pointMask">
            <el-input
              v-model="kingFilters.pointMask"
              style="width: 320px"
              clearable
              :placeholder="isEn ? 'e.g., Tag*' : '例如：Tag*，* 表示任意字符'"
            />
          </el-form-item>
          <el-form-item :label="t('dataIn.kinghist.tag')" prop="tags">
            <el-select
              v-model="kingFilters.tags"
              multiple
              filterable
              :loading="kingLoading.tags"
              style="width: 320px"
              clearable
              collapse-tags
              :max-collapse-tags="10"
              @change="onTagsChange"
            >
              <el-option :value="ALL_TAGS" :label="selectAllLabel" />
              <el-option :value="NONE_TAGS" :label="selectNoneLabel" />
              <el-option v-for="item in kingOptions.tags" :key="item.id" :label="item.label" :value="item.id" />
            </el-select>
          </el-form-item>
        </el-form>
      </div>
      <template #footer>
        <span class="dialog-footer">
          <el-button @click="kingDialogVisible = false">{{ t('common.cancel') }}</el-button>
          <el-button type="primary" :loading="kingSubmitLoading" @click="submitKingDownload">{{
            t('common.confirm')
          }}</el-button>
        </span>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { FormInstance, ElMessage } from 'element-plus';
import UploadCsv from './uploadCsv.vue';
import DocsContent from 'components/MdRender.vue';
import { getDataInProps } from '../model/useDataIn';
import { downloadByData, downloadByUrl } from 'utils/files';
import { isEn } from 'config';
import useSearchPoint from '../model/useSearchPoint';
import { t } from 'locales';
import { cloneDeep } from 'lodash-es';
import {
  currentPageType,
  sourceForm,
  connectivityCheckResult,
  validOpcFileResult,
  taskId,
  validateFormFields,
  formatFromData,
  isShowDatasetTable,
  datasetTableData
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
const progressVisible = ref<boolean>(false);
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
  value: string | number;
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
const isKinghist = computed(() => sourceForm.type === 'kinghist');
const agentId = computed(() => sourceForm.agent);
// 统一获取 update_mode：不同数据源/语言包下分组字段名可能不同
const updateMode = computed<string | undefined>(() => {
  try {
    const ga = (sourceForm as any)?.data?.groups_after;
    if (!ga || typeof ga !== 'object') return undefined;
    // 常规 key
    if (ga.collect_options && typeof ga.collect_options === 'object') {
      return ga.collect_options.update_mode as string;
    }
    // 兜底：在 groups_after 下寻找包含 update_mode 的对象
    for (const key of Object.keys(ga)) {
      const v = ga[key];
      if (v && typeof v === 'object' && 'update_mode' in v) {
        return (v as any).update_mode as string;
      }
    }
  } catch (_e) {
    // ignore
  }
  return undefined;
});
const namespaceList = computed(() => {
  const { namespaces = [] } = connectivityCheckResult.value;
  return namespaces.map((item, index) => ({ label: item, value: index }));
});

const isShowAddOpcPoint = computed(() => {
  // 重新上传了一个 csv,此时的任务还没有提交，因此csv没有生效，所有也不应该显示增加点位按钮
  return (
    isOpc.value &&
    oldValue.value &&
    props.modelValue != '*' &&
    props.modelValue === oldValue.value &&
    isEdit.value &&
    // 当“点位更新模式”为 none 时，不显示“增加数据点位”按钮
    (updateMode.value ?? 'none') !== 'none'
  );
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

onBeforeUnmount(() => {
  // Clear polling timer and reset preview state to avoid lingering loading/cached data
  if (timer.value) {
    clearInterval(timer.value);
  }
  loading.value = false;
  isShowDatasetTable.value = false;
  datasetTableData.value = undefined;
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

function openKingDialog() {
  validateFormFields(sourceParent?.refs.formRef, async () => {
    kingDialogVisible.value = true;
    // 一次性加载分组与标签
    await fetchKingHistPointOptions('');
  });
}

// 下载数据点位准备阶段
async function submit() {
  if (requestIng.value) return;
  try {
    if (!dataInProps.dataSource.api.getPointOptionsApi) return;
    requestIng.value = true;

    const via = sourceForm.agent;
    // normalize namespaces: join array to comma-separated string for backend DSN parsing
    const namespaces = Array.isArray(info.namespaces)
      ? info.namespaces.map(v => String(v)).join(',')
      : (info.namespaces as unknown as string) || '';

    const fromJson: any = cloneDeep(sourceForm);
    // Keep previous behavior: merge filter fields at top-level
    fromJson.root = info.root;
    fromJson.namespaces = namespaces;
    fromJson.pattern = info.pattern;
    // Critical fix: when downloading OPC points, ignore csv_config_file to force pulling from OPC server
    if (['opcua', 'opcda'].includes(fromJson.type) && fromJson.data) {
      // flat location (older structure)
      delete fromJson.data.csv_config_file;
      delete fromJson.data.csv_config_file_origin;
      // nested under datasets (current structure)
      if (fromJson.data.datasets && typeof fromJson.data.datasets === 'object') {
        delete fromJson.data.datasets.csv_config_file;
        delete fromJson.data.datasets.csv_config_file_origin;
      }
    }

    const params: Recordable = {
      from_json: fromJson,
      categories: category.value
    };

    if (via) {
      params.via = via;
    }

    progressVisible.value = true;
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
    progressVisible.value = false;
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

// 下载旧的上传文件（带上后端下载前缀）
function handleDownloadOldFile(file: { path: string; name?: string }) {
  const url = (dataInProps.downloadFileUrl || '') + file.path;
  downloadByUrl(url, file.name || 'download.csv');
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

// ---------------- KingHistorian download points ----------------
const kingDialogVisible = ref<boolean>(false);
const kingSubmitLoading = ref<boolean>(false);
const kingFormRef = ref<FormInstance>();

// Types for KingHistorian options
interface GroupNode {
  id: string;
  name: string;
  parentId?: string | null;
  children?: GroupNode[];
}

const kingFilters = reactive<{ group: string | null; pointMask: string; tags: string[] }>({
  group: null,
  pointMask: '',
  tags: []
});
// 选择模式：tags 保留多选，groups 为单选
const tagsMode = ref<'some' | 'all' | 'none'>('some');
const kingOptions = reactive<{ groups: GroupNode[]; tags: OptionItem[] }>({
  groups: [],
  tags: []
});
const kingLoading = reactive<{ groups: boolean; tags: boolean }>({
  groups: false,
  tags: false
});
const ALL_TAGS = '__ALL_TAGS__';
const NONE_TAGS = '__NONE_TAGS__';

// Paginated options for groups/points; avoid loading massive lists at once
type OptionItem = { id: string; label: string };
const groupOptions = ref<OptionItem[]>([]);
const tagAllIds = computed(() => kingOptions.tags.map(t => t.id));

// Label for Select-All; prefer i18n key under data.selectAll, fallback to Chinese "全选"
const selectAllLabel = computed(() => {
  try {
    const key = 'data.selectAll';
    const v = (t as any)(key);
    // 如果返回值依旧是 key（未配置翻译），则使用中英文兜底
    if (typeof v === 'string' && v && v !== key) return v;
  } catch (_e) {
    // ignore
  }
  return isEn.value ? 'Select all' : '全选';
});

// Label for Select-None; prefer i18n key under data.selectNone, fallback to Chinese "全不选"
const selectNoneLabel = computed(() => {
  try {
    const key = 'data.selectNone';
    const v = (t as any)(key);
    if (typeof v === 'string' && v && v !== key) return v;
  } catch (_e) {
    // ignore
  }
  return isEn.value ? 'Select none' : '全不选';
});

function onTagsChange(vals: string[]) {
  if (vals.includes(ALL_TAGS)) {
    tagsMode.value = 'all';
    kingFilters.tags = tagAllIds.value.slice();
  } else if (vals.includes(NONE_TAGS)) {
    tagsMode.value = 'none';
    kingFilters.tags = [];
  } else {
    tagsMode.value = vals.length ? 'some' : 'none';
    kingFilters.tags = vals;
  }
}

// 移除单独 groups 拉取逻辑，统一在 fetchKingHistPointOptions 中一次性获取

// Fetch options for groups & tags
async function fetchKingHistPointOptions(query?: string) {
  if (!dataInProps.dataSource.api.getPointOptionsApi) return;
  kingLoading.groups = true;
  kingLoading.tags = true;
  try {
    const payload: Record<string, any> = {
      from_json: formatFromData(sourceForm),
      categories: ['groups', 'tags'],
      pattern: query ?? '',
      offset: 0,
      limit: 300 // groups < 200, tags ~10, 300 足够一次性获取
    };
    if (agentId.value) payload.via = agentId.value;
    const res = await dataInProps.dataSource.api.getPointOptionsApi(payload);
    if (res && typeof res === 'object') {
      if (Array.isArray(res.groups)) {
        kingOptions.groups = normalizeGroupTree(res.groups);
        groupOptions.value = res.groups.map((g: any) => ({
          id: String(g.id ?? g.name),
          label: String(g.name ?? g.id)
        }));
      }
      if (Array.isArray(res.tags)) {
        kingOptions.tags = normalizeTags(res.tags);
      }
    }
  } finally {
    kingLoading.groups = false;
    kingLoading.tags = false;
  }
}

// ----- Normalizers for backend payloads -----
function normalizeGroupTree(raw: any[]): GroupNode[] {
  const recur = (nodes: any[]): GroupNode[] =>
    (nodes || []).map(n => ({
      id: String(n.id),
      name: String(n.name ?? n.id),
      parentId: undefined,
      children: recur(n.groups || [])
    }));
  return recur(raw);
}

function normalizeTags(raw: any[]): OptionItem[] {
  return (raw || []).map((t: any) => {
    const id = String(t.id ?? t.name ?? t.value ?? '');
    // English label prefers value/name/id; Chinese label prefers name_cn then falls back
    const enLabel = String(t.value ?? t.name ?? t.id ?? '');
    const zhLabel = String(t.name_cn ?? t.value ?? t.name ?? t.id ?? '');
    const label = isEn.value ? enLabel : zhLabel;
    return { id, label } as OptionItem;
  });
}

async function submitKingDownload() {
  if (kingSubmitLoading.value) return;
  kingSubmitLoading.value = true;
  try {
    // 1) 构建带 filters 的 DSN（拼接到 from_json.params 中）
    const dsn: any = formatFromData(sourceForm) || {};
    const ensureParams = (obj: any) => {
      if (!obj.params || typeof obj.params !== 'object') obj.params = {};
      return obj.params;
    };
    const paramsObj = ensureParams(dsn);

    // 选择模式：all/none/逗号分隔
    const encodeVal = (mode: 'all' | 'none' | 'some', arr: string[]) => {
      if (mode === 'all') return 'all';
      if (mode === 'none') return 'none';
      return arr && arr.length ? arr.join(',') : 'none';
    };

    // groups 单选：未选为 none，选了即为具体 group id
    paramsObj.groups = (kingFilters.group && kingFilters.group.trim()) || 'none';
    paramsObj.tag_name_mask = (kingFilters.pointMask || '').trim();
    paramsObj.tags = encodeVal(tagsMode.value, kingFilters.tags);

    // 2) 调用下载任务接口，走后端异步任务 + 轮询 + 下载
    const payload: Record<string, any> = {
      from_json: dsn,
      categories: 'points',
      lang: undefined as any
    };
    if (agentId.value) payload.via = agentId.value;

    progressVisible.value = true;
    const result = await dataInProps.dataSource.api.fechTicketApi(payload);
    ticket.value = result.ticket;

    // 轮询任务是否就绪
    timer.value = setInterval(async () => {
      const { complete } = await dataInProps.dataSource.api.checkReadyFile(result.ticket);
      completed.value = complete;
      const randomNum = Math.floor(Math.random() * 4);
      if (!complete) {
        percentage.value = percentage.value < 95 ? percentage.value + randomNum : 99;
      }
    }, 2000);
    kingDialogVisible.value = false;
  } catch (err: any) {
    ElMessage.error(err?.message || 'Request failed');
  } finally {
    kingSubmitLoading.value = false;
  }
}
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
  -moz-opacity: 0.5;
  opacity: 0.5;
}

:deep(.el-form-item) {
  margin-bottom: 18px;
}
</style>
