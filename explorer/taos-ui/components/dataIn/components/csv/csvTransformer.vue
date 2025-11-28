<template>
  <div class="csv-data">
    <div class="file-settings">
      <el-tabs v-model="localData.currentTab">
        <el-tab-pane
          :label="t('dataIn.uploadcsv')"
          name="upload_csv_file"
          :disabled="state.isModifying && !isAddable && localData.currentTab == 'monitor_file_directory'"
        >
          <div style="margin-bottom: 20px">
            <el-form
              ref="fileformRef"
              :model="localData.upload_csv_file"
              :rules="fileRules"
              label-width="240px"
              label-position="left"
            >
              <el-form-item prop="file_url">
                <template #label>
                  <el-tooltip placement="top" effect="light" :open-delay="0">
                    <template #content>
                      <DocsContent :content="t('dataIn.csvSelectFilesTip')" />
                    </template>
                    <span>
                      <span>{{ t('dataIn.csvSelectFiles') }}</span>
                      <span style="margin-left: 1px">
                        <Icon name="label_info" class="info-icon-custom"></Icon>
                      </span>
                    </span>
                  </el-tooltip>
                </template>
                <div class="upload-file">
                  <el-tooltip placement="top" effect="light" :open-delay="0" :disabled="!dataInProps.isCommunity">
                    <template #content>
                      <span v-dompurify-html="t('common.communityTip')"></span>
                    </template>
                    <el-upload
                      ref="upload"
                      class="upload-demo"
                      accept=".csv"
                      :label="t('dataIn.selectFile')"
                      :headers="uploadHeaders"
                      multiple
                      :on-remove="handleRemove"
                      :data="state.uploadData"
                      :action="dataInProps.uploadFileUrl"
                      :on-success="handleSuccess"
                      :before-upload="checkFileName"
                      :file-list="state.fileList"
                      :auto-upload="true"
                      :disabled="state.isModifying"
                      size="default"
                    >
                      <template #trigger>
                        <el-button
                          size="default"
                          :type="dataInProps.isIdmp ? 'default' : 'primary'"
                          plain
                          :disabled="dataInProps.isCommunity || state.isModifying"
                          >{{ t('dataIn.selectFile') }}</el-button
                        >
                      </template>
                    </el-upload>
                  </el-tooltip>
                  <span v-if="state.showfiletip" style="font-size: 12px; color: red">{{
                    t('dataIn.uploadcsvtip')
                  }}</span>
                </div>
              </el-form-item>
              <el-form-item prop="keep_processed_files" class="hidden-required">
                <template #label>
                  <el-tooltip placement="top" effect="light" :open-delay="0">
                    <template #content>
                      <DocsContent :content="t('dataIn.csvKeepProcessedFileDesc')" />
                    </template>
                    <span>
                      <span>{{ t('dataIn.csvKeepProcessedFile') }}</span>
                      <span style="margin-left: 1px">
                        <Icon name="label_info" class="info-icon-custom"></Icon>
                      </span>
                    </span>
                  </el-tooltip>
                </template>
                <el-switch v-model="localData.keep_processed_files"> </el-switch>
              </el-form-item>
            </el-form>
          </div>
        </el-tab-pane>
        <el-tab-pane
          v-if="!dataInProps.isCloud"
          :label="t('dataIn.configcsv')"
          name="monitor_file_directory"
          :disabled="state.isModifying && !isAddable && localData.currentTab == 'upload_csv_file'"
        >
          <el-form
            ref="fileformRef"
            :model="localData.monitor_file_directory"
            :rules="fileRules"
            label-width="240px"
            label-position="left"
          >
            <el-form-item prop="file_url">
              <template #label>
                <el-tooltip placement="top" effect="light" :open-delay="0">
                  <template #content>
                    <DocsContent :content="t('dataIn.csvFileDesc')" />
                  </template>
                  <span>
                    <span>{{ t('dataIn.csvFileDir') }}</span>
                    <span style="margin-left: 1px">
                      <Icon name="label_info" class="info-icon-custom"></Icon>
                    </span>
                  </span>
                </el-tooltip>
              </template>
              <el-input
                id="fileurl"
                v-model="localData.monitor_file_directory.file_url"
                size="default"
                :disabled="state.isModifying"
              ></el-input>
            </el-form-item>
            <el-form-item prop="filepattern" class="hidden-required">
              <template #label>
                <el-tooltip placement="top" effect="light" :open-delay="0">
                  <template #content>
                    <DocsContent :content="t('dataIn.csvFilePatternDesc')" />
                  </template>
                  <span>
                    <span>{{ t('dataIn.csvFilePattern') }}</span>
                    <span style="margin-left: 1px">
                      <Icon name="label_info" class="info-icon-custom"></Icon>
                    </span>
                  </span>
                </el-tooltip>
              </template>
              <el-input
                id="filepattern"
                v-model="localData.monitor_file_directory.file_pattern"
                size="default"
                :disabled="state.isModifying"
              ></el-input>
            </el-form-item>
            <el-form-item prop="filenotify" class="hidden-required">
              <template #label>
                <el-tooltip placement="top" effect="light" :open-delay="0">
                  <template #content>
                    <DocsContent :content="t('dataIn.csvNewFileNotifyDesc')" />
                  </template>
                  <span>
                    <span>{{ t('dataIn.csvNewFileNotify') }}</span>
                    <span style="margin-left: 1px">
                      <Icon name="label_info" class="info-icon-custom"></Icon>
                    </span>
                  </span>
                </el-tooltip>
              </template>
              <el-switch v-model="localData.monitor_file_directory.new_file_notify"> </el-switch>
            </el-form-item>
            <el-form-item
              v-if="localData.monitor_file_directory.new_file_notify"
              prop="notifyinterval"
              class="hidden-required"
            >
              <template #label>
                <el-tooltip placement="top" effect="light" :open-delay="0">
                  <template #content>
                    <DocsContent :content="t('dataIn.csvNotifyIntervalDesc')" />
                  </template>
                  <span>
                    <span>{{ t('dataIn.csvNotifyInterval') }}</span>
                    <span style="margin-left: 1px">
                      <Icon name="label_info" class="info-icon-custom"></Icon>
                    </span>
                  </span>
                </el-tooltip>
              </template>

              <el-input-number
                id="notifyinterval"
                v-model="localData.monitor_file_directory.notify_interval"
                size="default"
                :min="1"
                :max="600"
              >
              </el-input-number>
              <span style="margin-left: 10px">{{ t('dataIn.seconds') }}</span>
            </el-form-item>
            <el-form-item prop="keep_processed_files" class="hidden-required">
              <template #label>
                <el-tooltip placement="top" effect="light" :open-delay="0">
                  <template #content>
                    <DocsContent :content="t('dataIn.csvKeepProcessedFileDesc')" />
                  </template>
                  <span>
                    <span>{{ t('dataIn.csvKeepProcessedFile') }}</span>
                    <span style="margin-left: 1px">
                      <Icon name="label_info" class="info-icon-custom"></Icon>
                    </span>
                  </span>
                </el-tooltip>
              </template>
              <el-switch v-model="localData.keep_processed_files"> </el-switch>
            </el-form-item>
            <el-form-item prop="filesort" class="hidden-required">
              <template #label>
                <el-tooltip placement="top" effect="light" :open-delay="0">
                  <template #content>
                    <DocsContent :content="t('dataIn.csvFileSortDesc')" />
                  </template>
                  <span>
                    <span>{{ t('dataIn.csvFileSort') }}</span>
                    <span style="margin-left: 1px">
                      <Icon name="label_info" class="info-icon-custom"></Icon>
                    </span>
                  </span>
                </el-tooltip>
              </template>
              <el-radio-group v-model="localData.monitor_file_directory.sort">
                <el-radio value="1">{{ t('dataIn.sortasc') }}</el-radio>
                <el-radio value="2">{{ t('dataIn.sortdesc') }}</el-radio>
              </el-radio-group>
            </el-form-item>
          </el-form>
        </el-tab-pane>

        <el-tooltip placement="top" effect="light" :open-delay="0" :disabled="!dataInProps.isCommunity">
          <template #content>
            <span v-dompurify-html="t('common.communityTip')"></span>
          </template>
          <el-button
            type="primary"
            size="default"
            class="nextbtn"
            :loading="state.loading"
            :disabled="dataInProps.isCommunity"
            @click="getCsvColumnsData"
            >{{ t('dataIn.csvNext') }}</el-button
          >
        </el-tooltip>
        <CommonTransformer
          v-if="state.showTransformer"
          ref="transformRef"
          :parser-columns="state.extractArr"
        ></CommonTransformer>
      </el-tabs>
    </div>
  </div>
</template>
<script setup lang="ts">
import { ElMessage } from 'element-plus';
import { getCSVOptions, sourceForm } from '../../model/util';
import CommonTransformer from '../commonTransformer/index.vue';
import DocsContent from 'components/MdRender.vue';
import { getDataInProps, uploadHeaders } from 'components/dataIn/model/useDataIn';
import { currentPageType, taskId } from 'components/dataIn/model/util';
import { transformerState } from '../commonTransformer/util';
import { CsvTransformerParserType } from '../commonTransformer/type';
import { isEn } from 'config';
import { t } from 'locales';

const dataInProps = getDataInProps();

const props = defineProps<{
  modelValue: Recordable;
}>();

const localData: any = reactive({
  currentTab: 'upload_csv_file',
  path: '',
  keep_processed_files: false,
  monitor_file_directory: {
    file_url: '',
    file_pattern: '',
    new_file_notify: false,
    notify_interval: 30,
    sort: '1'
  },
  upload_csv_file: {
    file_url: ''
  }
});
const emit = defineEmits(['update:modelValue']);
watch(
  localData,
  newData => {
    const fileUrl =
      newData.currentTab == 'upload_csv_file'
        ? newData.upload_csv_file.file_url
        : newData.monitor_file_directory.file_url;
    newData.path = fileUrl;
    emit('update:modelValue', newData);
  },
  { deep: true }
);
interface stateProps {
  isModifying: boolean;
  showfiletip: boolean;
  maptypes: string[];
  showTransformer: boolean;
  transformerParser: Recordable | null;
  uploadData: Recordable;
  currentKey: {
    primary: '';
  };
  fileList: any[];
  extractArr: Recordable[];
  loading: boolean;
}
const state = reactive<stateProps>({
  isModifying: false,
  showfiletip: false,
  maptypes: ['value', 'generator', 'join', 'format', 'sum', 'expr'],
  showTransformer: false,
  transformerParser: null,
  uploadData: {
    req_id: new Date().getTime()
  },
  currentKey: {
    primary: ''
  },
  fileList: [],
  extractArr: [],
  loading: false
});

const fileformRef = ref();
const transformRef = ref();

const fileRules = reactive({
  file_url: [
    {
      required: true,
      message: t('dataIn.inputcsvdir')
    },
    {
      pattern: /^[\u4e00-\u9fa5A-Za-z0-9 %$@._\-/()[\]{}（）【】｛｝]+$/,
      message: t('dataIn.fileurlTip')
    }
  ]
});
const isAddable = computed(() => currentPageType.value === 'add');
const isCopyable = computed(() => currentPageType.value === 'copy');

watch(isEn, () => {
  nextTick(() => {
    state.showfiletip = false;
  });
});

onMounted(async () => {
  if (props.modelValue && props.modelValue.currentTab) {
    echoCsvData();
  }
});

function echoCsvData() {
  state.isModifying = !isCopyable.value && Number(taskId.value) > 0;

  localData.currentTab = props.modelValue.currentTab;
  localData.path = props.modelValue.path;
  localData[localData.currentTab] = JSON.parse(JSON.stringify(props.modelValue[localData.currentTab]));

  // 回显上传的文件列表
  if (localData.currentTab == 'upload_csv_file') {
    state.fileList = localData.upload_csv_file?.file_url
      .toString()
      .split(',')
      .map((item: any, index: any) => {
        return {
          name: item.substr(item.lastIndexOf('/') + 1),
          percentage: 100,
          raw: File,
          response: [].concat(item),
          size: 87,
          status: 'success',
          uid: index
        };
      });
  }
  //编辑状态直接从返回值去csv 的parser
  const rawData = transformerState.csvParser?.input;
  if (rawData && rawData.length > 0) {
    const csvColumns = [];
    const sample_values = [];
    for (const key in rawData[0]) {
      csvColumns.push(key);
    }
    for (let i = 0; i < rawData.length; i++) {
      const row = [];
      for (const key in rawData[i]) {
        row.push(rawData[i][key]);
      }
      sample_values.push(row);
    }
    formatCsvTransformerData(csvColumns, sample_values);
  }
}

function submitUrl() {
  let flag = false;
  fileformRef.value?.validate((valid: boolean) => {
    if (valid) {
      flag = true;
    } else {
      flag = false;
    }
  });
  return flag;
}
function handleRemove(_file: any, filelist: []) {
  state.fileList = filelist;
}

function handleSuccess(_response: any, _file: any, fileList: []) {
  state.fileList = fileList;
  state.showfiletip = false;
  localData.upload_csv_file.file_url = dataInProps.isCloud
    ? _response.data.join(',')
    : fileList
      .filter((item: any) => item.response)
      .map((item: any) => item.response[0])
      .join(',');
}
function csvFileInputOK() {
  if (localData.currentTab == 'upload_csv_file' && state.fileList.length == 0) {
    state.showfiletip = true;
    // ElMessage.warning(t('dataIn.uploadcsvtip'));
    return false;
  } else if (localData.currentTab == 'monitor_file_directory' && !localData.monitor_file_directory.file_url) {
    // ElMessage.warning(t('dataIn.inputcsvdir'));
    submitUrl();
    return false;
  }
  return true;
}

function submitUpload() {
  let isbreak = csvFileInputOK();
  if (!isbreak) {
    return isbreak;
  }
  if (!state.showTransformer) {
    ElMessage.closeAll();
    ElMessage({
      type: 'warning',
      message: t('dataIn.transformer.nexttip')
    });

    isbreak = false;
  }

  return isbreak;
}

async function getCsvColumnsData() {
  try {
    state.loading = true;
    state.showfiletip = false;
    if (!csvFileInputOK()) {
      state.loading = false;
      return;
    }

    state.showTransformer = false;
    transformerState.csvTransformerParser = null;

    const parseParam = getCsvParseParam();
    const fileUrl =
      localData.currentTab == 'upload_csv_file'
        ? state.fileList.map((item: any) => item.response[0]).join(',')
        : localData.monitor_file_directory.file_url;

    const result = await dataInProps.transform.api.getCSVColumns(fileUrl, 'csv', parseParam);
    state.loading = false;
    if (result && result.message) {
      ElMessage.error(result.message);
      return;
    }

    const columns = result.file_header.column_names;
    const columnInObj: Recordable = {};
    for (let i = 0; i < columns.length; i++) {
      if (columns[i] === '') {
        ElMessage.error(t('dataIn.transformer.emptyColumnName') + columns.join(', '));
        return;
      }
      if (columnInObj[columns[i]]) {
        ElMessage.error(t('dataIn.transformer.duplicateColumnName') + columns[i]);
        return;
      }
      columnInObj[columns[i]] = true;
    }

    if (result && !result.sample_values) {
      ElMessage.error(t('dataIn.transformer.emptySampleValues'));
      return;
    }

    formatCsvTransformerData(columns, result.sample_values ?? []);
    submitUpload();
  } catch (error: any) {
    state.loading = false;
    error && error.message && ElMessage.error(error.message);
  }
}
//组合CSV的transfomrer页面需要的数据
function formatCsvTransformerData(columns: string[], values: any[]) {
  const inputList = values.map(item => {
    return Object.fromEntries(
      item.map((val: any, index: number) => {
        return [columns[index], val];
      })
    );
  });
  const msgBody = values.map(item => {
    return item;
  });
  msgBody.unshift(columns.toString());
  state.extractArr.splice(0, state.extractArr.length);
  columns.forEach(item => {
    const obj: Recordable = {};
    obj['columns'] = columns.map(() => {
      return {
        description: item,
        name: item,
        show: true,
        type: 'varchar',
        value: ''
      };
    });
    (obj['columnname'] = ''), (obj['expression'] = ''), (obj['type'] = '');
    state.extractArr.push(obj);
  });
  const csvTransformer = {
    columns: transformerState.csvTransformerlocalCols.length > 0 ? transformerState.csvTransformerlocalCols : columns,
    inputList: transformerState.csvParser ? transformerState.csvParser.input : inputList,
    msgBody: msgBody.join('\n')
  };
  const transformerColumns = [
    {
      value: 'expression',
      label: t('expression'),
      children: state.maptypes.map(item => {
        return {
          value: item,
          label: item
        };
      })
    },
    {
      value: 'mapping',
      label: t('mapping'),
      children: csvTransformer['columns'].map(item => {
        return {
          value: item,
          label: item
        };
      })
    }
  ];

  transformerState.transformerMapColumns = transformerColumns;
  transformerState.csvTransformerParser = csvTransformer as CsvTransformerParserType;
  state.showTransformer = true;
}

//获取 csv 解析需要的参数
function getCsvParseParam() {
  const options = getCSVOptions(sourceForm.data);
  if (localData.currentTab == 'monitor_file_directory') {
    options.push(`file_pattern=${localData.monitor_file_directory.file_pattern}`);
  }
  return options.join('&');
}
function checkFileName(file: any) {
  const regex = /^[\u4e00-\u9fa5A-Za-z0-9 %$@._\-()[\]{}（）【】｛｝]+$/;
  const fileName = file.name;
  if (!regex.test(fileName)) {
    ElMessage.error(t('dataIn.supportCharacter'));
    return false; // 不允许上传
  }

  for (let i = 0; i < state.fileList.length; i++) {
    if (state.fileList[i].name === fileName) {
      if (!confirm('有重名文件，是否要覆盖文件？')) {
        return false;
      }
    }
  }

  return true; // 允许上传
}

defineExpose({
  submitParse: submitUpload
});
</script>
<style lang="scss" scoped>
$color-description: rgb(137 130 130);

:deep(.markdown-body) {
  p {
    font-size: 14px;
  }

  color: $color-description;
}

.upload-demo {
  width: 300px;
}

.csv-data {
  .el-upload {
    text-align: left;
  }

  .upload-file {
    align-items: baseline;

    .label {
      position: relative;
      width: 220px;
      padding-right: 40px;
      font-size: 14px;
      font-weight: 500;
      color: #4259ce;
      text-align: left;

      &.required {
        padding-left: 10px;

        &::before {
          position: absolute;
          left: 0;
          font-size: 16px;
          line-height: 25px;
          color: red;
          content: '*';
        }

        &.en {
          width: 225px;
        }
      }
    }

    .el-input {
      flex: 1;
    }
  }

  .nextbtn {
    width: 100%;
  }

  .csv-config {
    margin-bottom: 20px;

    .csv-tableheader {
      display: grid;
      grid-template-columns: 1fr 1.5fr 1.5fr 1fr 1fr 1fr;
      column-gap: 10px;
      padding-top: 5px;
      padding-bottom: 5px;
      background-color: #f5f7fa;
      border-bottom: none;

      li {
        display: flex;
        justify-content: center;
        font-size: 16px;
        color: #909399;
      }
    }

    .csv-content {
      display: grid;
      grid-template-columns: 1fr auto;
      border-bottom: 1px solid #ebeef5;

      .csv-col {
        display: flex;
        align-items: center;
        justify-content: center;
        width: 123px;

        &:first-child {
          padding-left: 10px;
        }
      }
    }
  }
}
</style>
