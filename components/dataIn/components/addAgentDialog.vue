<template>
  <Dialog v-model="visible" v-bind="dialogConfig" @close="emit('close', $event)">
    <AgentCreate v-bind="dbFormProps" @close="emit('close', $event)" @update="emit('update', $event)" />
  </Dialog>
</template>

<script setup lang="ts">
import AgentCreate from '../views/agent/addAgent.vue';
import Dialog from 'components/Dialog.vue';
import { t } from 'locales';

const props = withDefaults(
  defineProps<{
    agentList: Recordable[];
    modelValue: boolean;
  }>(),
  {
    agentList: () => [],
    modelValue: false
  }
);
const dialogConfig = reactive({
  config: {
    width: '620px',
    title: t('dataIn.createNewAgent'),
    // 'append-to-body': true,
    'close-on-press-escape': false,
    'close-on-click-modal': false,
    'destroy-on-close': true
  }
});

const emit = defineEmits(['update:modelValue', 'close', 'update']);

const visible = computed({
  get: () => props.modelValue,
  set: val => emit('update:modelValue', val)
});

type Props = InstanceType<typeof AgentCreate>['$props'];

const dbFormProps: Props = {
  agentList: props.agentList
};
</script>
