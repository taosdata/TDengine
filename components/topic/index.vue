<template>
  <div class="page-wrapper">
    <PageHeader :title="t('topic.pageTitle')"></PageHeader>
    <section class="content">
      <RouterTabs :tabs="currentTabs" />
      <router-view></router-view>
    </section>
  </div>
</template>

<script lang="ts" setup>
import RouterTabs from '../RouterTabs.vue';
import { t } from 'locales';
const props = withDefaults(
  defineProps<{
    showShareTopic?: boolean;
  }>(),
  {
    showShareTopic: false
  }
);
const allTabs = [
  {
    label: t('common.topic'),
    name: '/topic'
  },
  {
    label: t('common.consumer'),
    name: '/topic/consumer'
  },
  {
    label: t('topic.shareTopic'),
    name: '/topic/share'
  },
  {
    label: t('common.sampleCode'),
    name: '/topic/example'
  }
];
const currentTabs = computed(() => {
  return props.showShareTopic ? allTabs : allTabs.filter(tab => tab.name !== '/topic/share');
});
</script>

<style scoped lang="scss"></style>
