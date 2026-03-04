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
      <el-input v-model="localData[config.field]" style="flex: 1" :placeholder="config.placeholder">
      </el-input>
    </template>
  </div>
</template>

<script setup lang="ts">
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
  if (
    currentTaskStatus.value == 'created' &&
    sourceForm.type == 'kafka' &&
    (props.config.field == 'group' || props.config.field == 'client_id')
  ) {
    return false;
  }
  return currentPageType.value == 'edit';
});
const isCopy = computed(() => {
  return currentPageType.value == 'copy';
});

/** Generate random 8-char alphanumeric string for MQTT client_id suffix */
function randomIdSuffix(): string {
  const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
  let result = '';
  for (let i = 0; i < 8; i++) {
    result += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return result;
}

const emit = defineEmits(['update:data']);
watch(localData, newData => {
  emit('update:data', newData);
});

onMounted(() => {
  if (isEdit.value) {
    // Compatible with legacy task: show task id for group
    localData['group'] = props.data['group'] || taskId.value;
  }
  if (isCopy.value) {
    if (props.config.field === 'client_id') {
      // Auto-generate new client_id when copying MQTT task
      localData[props.config.field] = `taosx_client_${randomIdSuffix()}`;
    } else {
      // Clear group when copying Kafka task
      localData[props.config.field] = '';
    }
  }
  // Auto-generate MQTT client_id on create: taosx_client_ + 8 random chars
  if (!isEdit.value && !isCopy.value && props.config.field === 'client_id' && !localData[props.config.field]) {
    localData[props.config.field] = `taosx_client_${randomIdSuffix()}`;
  }
});
</script>

<style scoped lang="scss"></style>
