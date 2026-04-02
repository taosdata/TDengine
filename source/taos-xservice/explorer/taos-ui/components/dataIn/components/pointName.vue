<template>
  <div class="flex-start flex-1">
    <el-input v-model="localData[config.field]" style="flex: 1" class="mr20" :placeholder="config.placeholder">
    </el-input>
    <el-tooltip placement="top" effect="light" :open-delay="0" :disabled="!dataInProps.isCommunity">
      <template #content>
        <span v-dompurify-html="t('common.communityTip')"></span>
      </template>
      <el-button :loading="loading" :disabled="loading || dataInProps.isCommunity" type="primary" @click="search">{{
        buttonText
      }}</el-button>
    </el-tooltip>
  </div>
</template>

<script setup lang="ts">
import { getDataInProps } from '../model/useDataIn';
import useSearchPoint from '../model/useSearchPoint';
import { t } from 'locales';
const dataInProps = getDataInProps();
const { loading, search } = useSearchPoint();
const props = withDefaults(
  defineProps<{
    config: Record<string, any>;
    data: Record<string, any>;
  }>(),
  {}
);
const localData = reactive(props.data);

const buttonText = computed(() => props.config?.viewText || t('dataIn.transformer.preview'));

const emit = defineEmits(['update:data']);
watch(localData, newData => {
  emit('update:data', newData);
});
</script>

<style scoped lang="scss"></style>
