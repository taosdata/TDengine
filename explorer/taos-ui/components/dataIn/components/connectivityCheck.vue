<template>
  <div class="box-check-connectivity">
    <el-tooltip placement="top" effect="light" :open-delay="0" :disabled="!dataInProps.isCommunity">
      <template #content>
        <span v-dompurify-html="t('common.communityTip')"></span>
      </template>
      <section v-if="isView && !dataInProps.isCommunity" class="block-wrapper">
        <BlockHeader :title="t('dataIn.check')"></BlockHeader>
      </section>
      <el-button
        :loading="checkLoading"
        :disabled="dataInProps.isCommunity"
        class="btn-check-connectivity"
        type="primary"
        size="default"
        plain
        @click.capture.stop="clickCheckBtn"
        >{{ t('dataIn.check') }}
      </el-button>
    </el-tooltip>
    <div v-show="JSON.stringify(checkResult) !== '{}'" class="text">
      <el-icon :size="18" :color="content.icon === 'SuccessFilled' ? '#33b169' : '#ff2e4d'" class="mr10">
        <component :is="content.icon" />
      </el-icon>
      <div class="flex-wrap">
        <span v-if="content.contentText" :class="[lang]">{{ content.contentText }}</span>
        <span v-if="content.messageText" :class="['error', lang]">{{ content.messageText }}</span>
      </div>
    </div>
  </div>
</template>
<script setup lang="ts">
// 功能 做连通性检查 获取某些数据 只校验在某些字段
import type { ComponentInternalInstance } from 'vue';
import BlockHeader from './blockHeader.vue';
import { t } from 'locales';
import {
  currentPageType,
  sourceForm,
  connectivityCheckResult,
  validateFormFields,
  formatFromData
} from '../model/util';
import { getDataInProps } from '../model/useDataIn';
import { isEn } from 'config';
import axios from 'axios';
const dataInProps = getDataInProps();

const sourceParent = inject<ComponentInternalInstance>('sourceParent') as any;
const toUrl: any = inject('toUrl');

const checkResult: any = ref({});
const checkLoading = ref<boolean>(false);

const type = computed(() => {
  return sourceForm.type;
});
const isEdit = computed(() => {
  return currentPageType.value == 'edit';
});
const isView = computed(() => {
  return currentPageType.value == 'view';
});
const isCopy = computed(() => {
  return currentPageType.value == 'copy';
});
const content = computed(() => {
  let contentText = '';
  let messageText = '';
  let icon = '';
  const { valid, support, version, message } = checkResult.value;
  if (valid) {
    if (support) {
      contentText = version ? t('dataIn.successVersionTip', [version]) : t('dataIn.successTip');
      icon = 'SuccessFilled';
    } else {
      contentText = t('dataIn.unSupportTip', [version]);
      icon = 'CircleCloseFilled';
    }
  } else {
    contentText = t('dataIn.failTip');
    messageText = t('dataIn.errorMessage') + message;
    icon = 'CircleCloseFilled';
  }
  return { contentText, messageText, icon };
});

const lang = computed(() => {
  return isEn.value ? 'en-text' : 'zh-text';
});

watch(type, () => {
  checkResult.value = {};
});

onMounted(() => {
  if (isEdit.value) {
    if (isCopy.value && type.value === 'opcua') {
      clickCheckBtn();
    } else if (!isCopy.value) {
      clickCheckBtn();
    }
  }

  if (isView.value) {
    const agent = sourceForm.agent;
    getValidateResult(sourceForm, agent);
  }
});

function onValid(param: Recordable, agent: number) {
  getValidateResult(param, agent);
}

function clickCheckBtn() {
  checkResult.value = {};
  validateFormFields(sourceParent?.refs.formRef, onValid);
}
// 数据源可用性和版本检查
async function getValidateResult(data: Recordable, agent: number | string) {
  try {
    checkLoading.value = true;
    let viaObj = {};
    if (agent) {
      viaObj = {
        via: agent
      };
    }

    const from_json = formatFromData(data);
    const parameter = {
      from_json,
      to: toUrl.value,
      ...viaObj
    };

    const result = await dataInProps.dataSource.api.connectivityCheckApi(parameter);
    checkResult.value = result;

    connectivityCheckResult.value = result;
    checkLoading.value = false; // 检测的 loading 效果
  } catch (error) {
    if (axios.isAxiosError(error)) {
      const message = error.message;
      checkResult.value.message = message;
    }
    checkLoading.value = false;
  }
}

defineExpose({ clickCheckBtn });
</script>
<style lang="scss" scoped>
.box-check-connectivity {
  margin-bottom: 30px;

  .btn-check-connectivity {
    width: 100%;
  }
}

.block-wrapper {
  padding: 0 15px;
  border-radius: 12px;
}

.text {
  display: flex;
  align-items: center;
  padding: 10px;
  font-size: 14px;
  font-weight: 400;
  color: #16191f;
  text-align: left;
}

.flex-wrap {
  display: flex;
  flex-wrap: wrap;

  > span {
    display: inline-block;
    width: 100%;
    word-wrap: break-word;
    white-space: pre-wrap;
  }
}

.zh-text {
  word-break: break-all;
}

.en-text {
  word-break: keep-all;
}

.error {
  margin-top: 8px;
  color: red;
}
</style>
