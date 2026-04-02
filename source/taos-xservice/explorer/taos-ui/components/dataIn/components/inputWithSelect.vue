<template>
  <el-input
    v-model="localData[config.field]"
    :placeholder="config.placeholder"
    class="input-with-select"
    :disabled="disabledValue"
    @blur="trimInput"
  >
    <template #prepend>
      <el-select v-model="localData[config.field + '_unit']" style="width: 120px" @change="handleDatatype">
        <el-option
          v-for="item in options"
          :key="item.value"
          v-bind="item"
          :title="item.description"
          :disabled="item.disabled"
        ></el-option>
      </el-select>
    </template>
  </el-input>
  <el-button
    v-if="config.action"
    v-loading.fullscreen.lock="fullscreenLoading"
    plain
    style="margin-top: 10px"
    :type="dataInProps.isIdmp ? 'default' : 'primary'"
    size="default"
    :icon="`${config.action}`"
    @click="submitAction"
  >
    {{ config.action_text }}
  </el-button>
</template>

<script setup lang="ts">
import { getDataInProps } from '../model/useDataIn';
import { sourceForm } from '../model/util';
import { downloadByUrl } from 'utils/files';
import { t } from 'locales';

const dataInProps = getDataInProps();

const props = withDefaults(
  defineProps<{
    config: Record<string, any>;
    data: Record<string, any>;
    options: Record<string, any>;
  }>(),
  {}
);
const localData = reactive(props.data);

const fullscreenLoading = ref(false);

const emit = defineEmits(['update:data']);
watch(localData, newData => {
  emit('update:data', newData);
});

const disabledValue = computed(() => {
  return props.config.disabledValues
    ? props.config.disabledValues.includes(localData[props.config.field + '_unit'])
    : false;
});

function submitAction() {
  // const sourceForm = sourceParent.sourceForm;
  const type = sourceForm.type;
  // 目前只有 pi 数据源的 config.action=download 的按钮用到这个方法，不同的数据源需要不同的处理
  if (['pi', 'pibackfill'].indexOf(type) >= 0 && props.config.action === 'Download') {
    downloadPIDefaultConfigFile();
  } else {
    console.log('not support, please add your own logic here.');
  }
}

async function downloadPIDefaultConfigFile() {
  const via = sourceForm.agent;
  const params: Recordable = {
    from: sourceForm
  };
  if (via) {
    params.via = via;
  }

  try {
    fullscreenLoading.value = true;
    const defaultFileInfo = await dataInProps.dataSource.api.generatePIDefaultConfigFile(params);
    if (typeof defaultFileInfo !== 'string') {
      if (defaultFileInfo && defaultFileInfo.message) {
        ElMessage.error(defaultFileInfo.message);
      } else {
        ElMessage.error('Failed to generate default config file');
      }
      return;
    }

    const downloadUrl = dataInProps.downloadFileUrl + defaultFileInfo;
    downloadByUrl(downloadUrl);
    fullscreenLoading.value = false;

    if (localData.transform_config_file) {
      // 已经有配置文件情况下, 则询问是否覆盖
      ElMessageBox.confirm(t('dataIn.pi.confirmOverwriteConfigFile'), t('common.tips'), {
        confirmButtonText: t('common.yes'),
        cancelButtonText: t('common.no'),
        type: 'warning'
      }).then(() => {
        localData.transform_config_file = defaultFileInfo;
      });
    } else {
      // 如果当前使用默认配置，但是没有配置文件，则直接更新
      localData.transform_config_file = defaultFileInfo;
    }
  } finally {
    fullscreenLoading.value = false;
  }
}
function trimInput() {
  // 在失去焦点时去除输入框值的前后空格
  localData[props.config.field] = props.data[props.config.field].toString().trim();
}
function handleDatatype() {
  if (props.config.disabledValues && props.config.disabledValues.includes(localData[props.config.field + '_unit'])) {
    localData[props.config.field] = '';
  }
}
</script>

<style scoped lang="scss">
.input-with-select {
  :deep(.el-input-group__prepend) {
    background-color: #fff;
    border-color: #bebcbc;

    .el-input__inner {
      box-shadow: none;
    }
  }

  :deep(.el-input-group__append) {
    background-color: #fff;
    border-color: #bebcbc;

    .el-input__inner {
      box-shadow: none;
    }
  }

  /* 新增：统一表单项文本颜色（普通/禁用/placeholder） */
  :deep(.el-input__inner),
  :deep(.el-input.is-disabled .el-input__inner),
  :deep(.el-input__inner[disabled]),
  :deep(.el-input__inner::placeholder) {
    color: var(--el-text-color-regular) !important;
    -webkit-text-fill-color: var(--el-text-color-regular) !important;
    opacity: 1 !important;
  }

  :deep(.el-select__wrapper .el-select__selected-item),
  :deep(.el-select__wrapper.is-disabled .el-select__selected-item),
  :deep(.el-select .el-input__inner) {
    color: var(--el-text-color-regular) !important;
    -webkit-text-fill-color: var(--el-text-color-regular) !important;
    opacity: 1 !important;
  }
}
</style>
