<template>
  <Dialog v-model="visible" v-bind="dialogConfig" @close="emit('close', $event)">
    <DatabaseCreate v-bind="dbFormProps" @cancel="emit('close', $event)" @update="emit('update', $event)" />
  </Dialog>
</template>

<script setup lang="ts">
import DatabaseCreate from 'components/explorer/components/createDbForm.vue';
import Dialog from 'components/Dialog.vue';
// import { t } from 'locales';
import { instance } from 'config';

const props = withDefaults(
  defineProps<{
    dbList: Recordable[];
    modelValue: boolean;
    createApi: () => Promise<any>;
  }>(),
  {
    dbList: () => [],
    modelValue: false
  }
);
const dialogConfig = reactive({
  config: {
    width: '62%',
    // title: t('dataIn.createDatabase'),
    'append-to-body': true,
    'close-on-press-escape': false,
    'close-on-click-modal': false
  }
});

const emit = defineEmits(['update:modelValue', 'close', 'update']);
const visible = computed({
  get: () => props.modelValue,
  set: val => emit('update:modelValue', val)
});

type Props = InstanceType<typeof DatabaseCreate>['$props'];

const dbFormProps: Props = {
  dbList: props.dbList,
  updateApi: props.createApi,
  isEdit: false,
  version: instance.version
};
</script>
