<template>
  <div class="flex-start flex-1">
    <el-input
      v-if="isEdit && !isCopy"
      v-model="localData[config.field]"
      style="flex: 1"
      :placeholder="config.placeholder"
      :disabled="isEdit"
    >
    </el-input>
    <template v-else>
      <span>taosx</span>
      <el-input v-model="localData[config.field]" style="flex: 1" class="mr20 ml15" :placeholder="config.placeholder">
      </el-input>

      <el-tooltip placement="top" effect="light" :open-delay="0">
        <template #content>
          <span v-dompurify-html="t('dataIn.taskIdTip', [config.label])"></span>
        </template>
        <el-switch v-model="localData[switchField]" type="primary"></el-switch>
      </el-tooltip>
    </template>
  </div>
</template>

<script setup lang="ts">
import { t } from 'locales';
import { currentPageType, taskId, sourceForm, currentTaskStatus } from '../model/util';

const props = withDefaults(
  defineProps<{
    config: Record<string, any>;
    data: Record<string, any>;
  }>(),
  {}
);
const localData = reactive(props.data);

const isEdit = computed(() => {
  if (currentTaskStatus.value == 'created' && sourceForm.type == 'kafka' && (props.config.field == 'group' || props.config.field == 'client_id') ) {
    return false
  }
  return currentPageType.value == 'edit';
});
const isCopy = computed(() => {
  return currentPageType.value == 'copy';
});
const switchField = computed(() => {
  return `${props.config.field.startsWith('group') ? props.config.field + '_id' : props.config.field}_with_task_id`;
});

const emit = defineEmits(['update:data']);
watch(localData, newData => {
  emit('update:data', newData);
});

onMounted(() => {
  if (isEdit.value) {
    // 兼容历史任务的 group 回显任务 id
    localData['group'] = props.data['group'] || taskId.value;
  }
  if (isCopy.value) {
    // 复制时置空group/client_id
    localData[props.config.field] = '';
  }
});
</script>

<style scoped lang="scss"></style>
