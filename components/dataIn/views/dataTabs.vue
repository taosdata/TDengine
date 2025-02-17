<template>
  <div>
    <el-tabs v-model="activeName" @tab-click="tabClick">
      <el-tab-pane v-for="item in currentTabs" :key="item.comp" :name="item.comp" v-bind="item">
        <component :is="currentDataInComponent" :key="item.comp"></component>
        <slot></slot>
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<script setup lang="ts">
import { TabsPaneContext } from 'element-plus';
import { useRoute, useRouter } from 'hooks/useCurrentRouter';
import Task from './task/index.vue';
import Agent from './agent/index.vue';
import DataCollectionAgents from 'components/document/party.vue';
import { t } from 'locales';
const isOem = false;

const components = {
  Task,
  Agent,
  DataCollectionAgents
};
const route = useRoute();
const router = useRouter();
const activeName = ref('');
const currentDataInComponent = computed(() => components[activeName.value as keyof typeof components]);

watchEffect(() => {
  activeName.value = (route?.params.tab as string) ?? '';
});
const tabs = computed(() => {
  const result = [
    {
      label: t('dataIn.datasource'),
      comp: 'Task',
      isShow: true // 临时先解决先 tab 权限
    },
    {
      label: t('dataIn.agent'),
      comp: 'Agent',
      isShow: true
    },
    {
      label: t('dataIn.datacollection'),
      comp: 'DataCollectionAgents',
      isShow: !isOem
    }
  ];
  return result;
});

const currentTabs = computed(() => {
  return tabs.value.filter(tab => tab.isShow);
});

const tabClick = (tab: TabsPaneContext) => {
  router.push({
    path: (tab.paneName as string) ?? ''
  });
};
</script>

<style lang="scss" scoped></style>
