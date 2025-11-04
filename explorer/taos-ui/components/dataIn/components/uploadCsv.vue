<template>
  <div style="display: flex">
    <el-tooltip placement="top" effect="light" :open-delay="0" :disabled="!dataInProps.isCommunity">
      <template #content>
        <span v-dompurify-html="$t('common.communityTip')"></span>
      </template>
      <el-upload
        ref="upload"
        class="upload-csv"
        :headers="uploadHeaders"
        :data="uploadData"
        :accept="accept"
        :on-remove="handleRemove"
        :on-preview="handlePreview"
        :action="dataInProps.uploadFileUrl"
        :multiple="false"
        :on-success="handleSuccess"
        :on-change="handleChange"
        :file-list="files"
        :auto-upload="true"
      >
        <template #trigger>
          <el-button
            v-if="!isOpc || isOpcDsnValid"
            ref="uploadButtonRef"
            size="default"
            plain
            icon="Upload"
            :type="dataInProps.isIdmp ? 'default' : 'primary'"
            :disabled="dataInProps.isCommunity || disabled"
            >{{ btnText || t('dataIn.selectFile') }}</el-button
          >
        </template>
      </el-upload>
    </el-tooltip>
  </div>
</template>

<script setup lang="ts">
import { getDataInProps, uploadHeaders } from '../model/useDataIn';
import { currentPageType } from '../model/util';
import type { UploadRawFile, UploadFile } from 'element-plus';
import { downloadByUrl } from 'utils/files';
import { t } from 'locales';

type UploadCusFile = {
  name: string;
  path: string;
} & UploadFile;

const dataInProps = getDataInProps();
const props = withDefaults(
  defineProps<{
    config: Record<string, any>;
    modelValue: string;
    isOpc?: boolean;
    disabled?: boolean;
    btnText?: string;
    isOpcDsnValid?: boolean;
  }>(),
  {
    disabled: false,
    modelValue: '',
    btnText: '',
    isOpcDsnValid: false
  }
);

const emit = defineEmits(['update:modelValue', 'valid-opc-file']);

const files = ref<UploadCusFile[]>([]);

const uploadData = computed(() => {
  return { req_id: new Date().getTime() };
});
const isEdit = computed(() => currentPageType.value === 'edit');
const accept = computed(() => props.config.accept || '');

function init() {
  if (isEdit.value && props.modelValue) {
    emit('valid-opc-file');
  }
  // 在新增或者编辑时切换 tab 都能保持上传的文件列表
  handleFiles(props.modelValue);
}
init();

watch(
  () => props.modelValue,
  newFile => {
    handleFiles(newFile);
    update();
  }
);

function handleRemove(_: any, fileList: UploadCusFile[]) {
  files.value = fileList;
  update();
}
function handleChange() {}

function handlePreview(file: UploadCusFile) {
  const url = dataInProps.downloadFileUrl + file.path;
  downloadByUrl(url as string);
}

async function handleSuccess(response: any, file: UploadCusFile) {
  file.path = response[0];
  files.value = ([] as UploadCusFile[]).concat(file);
  update();
  emit('valid-opc-file');
}

function handleFiles(defaultFile: string) {
  if (defaultFile && defaultFile != '*') {
    const file: UploadCusFile = {
      name: defaultFile.substring(defaultFile.lastIndexOf('/') + 1),
      path: defaultFile.startsWith('@') ? defaultFile.substring(1) : defaultFile,
      percentage: 100,
      raw: {} as UploadRawFile,
      response: [defaultFile],
      size: 87,
      status: 'success',
      uid: 1
    };
    files.value = ([] as UploadCusFile[]).concat(file);
  }
}

function update() {
  emit(
    'update:modelValue',
    (files.value as UploadCusFile[])
      .filter(item => item.path)
      .map(item => '@' + item.path)
      .join(',')
  );
}
</script>

<style scoped lang="scss">
.upload-csv {
  display: flex;
  align-items: center;

  &:deep(.el-upload-list) {
    margin-top: 0;
  }

  &:deep(.el-upload-list__item) {
    margin: 0 1rem;
  }
}

:deep(.el-upload-list__item.is-success.focusing .el-icon-close-tip) {
  display: none !important;
}
</style>
