<template>
  <div class="relative">
    <section v-if="dataInProps.tasoxVersion" class="version-block">
      {{ t('dataIn.version') }}{{ dataInProps.tasoxVersion }}
    </section>
    <el-tabs v-model="activeName" @tab-click="tabClick">
      <el-tab-pane v-for="item in currentTabs" :key="item.key" :name="item.key" :label="item.label" lazy>
        <component :is="item.comp" :key="item.key"></component>
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
import { getDataInProps } from '../model/useDataIn';
const isOem = false;

const route = useRoute();
const router = useRouter();
const activeName = ref('');
const dataInProps = getDataInProps();

watchEffect(() => {
  activeName.value = (route?.params.tab as string) ?? '';
});
const tabs = computed(() => {
  const result = [
    {
      label: t('dataIn.datasource'),
      key: 'Task',
      comp: Task,
      isShow: true // 临时先解决先 tab 权限
    },
    {
      label: t('dataIn.agent'),
      key: 'Agent',
      comp: Agent,
      isShow: true
    },
    {
      label: t('dataIn.datacollection'),
      key: 'DataCollectionAgents',
      comp: DataCollectionAgents,
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

onMounted(() => {
  activeName.value = (route?.params.tab as string) ?? currentTabs.value[0].key;
});
</script>

<style lang="scss" scoped>
.version-block {
  position: absolute;
  top: 10px;
  right: 30px;
  z-index: 1;
}
</style>
