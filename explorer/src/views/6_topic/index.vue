<template>
  <div class="page-wrapper">
    <!-- <PageHeader :title="$t('topic.pageTitle')"></PageHeader> -->
    <section class="content">
      <el-tabs model-value="topic">
        <LinkTab :tabs="$IS_OEM ? oemTabs : tabs" class="topic-heads" />
        <router-view></router-view>
      </el-tabs>
    </section>
  </div>
</template>

<script setup lang="ts">
const { $IS_OEM } = inject('globalCustomProperties') as GlobalCustomProperties;

const { t } = useI18n();

provide('parentName', 'Topic');

const tabs = computed(() => {
  const tabs = [
    {
      label: t('topic.topic'),
      name: '/topic'
    },
    {
      label: t('topic.consumer'),
      name: '/topic/consumer'
    },
    {
      label: t('topic.shareTopic'),
      name: '/topic/share'
    },
    {
      label: t('topic.sampleCode'),
      name: '/topic/example'
    }
  ];
  return tabs;
});
const oemTabs = computed(() => {
  const oemTabs = [
    {
      label: t('topic.topic'),
      name: '/topic'
    },
    {
      label: t('topic.consumer'),
      name: '/topic/consumer'
    },
    {
      label: t('topic.shareTopic'),
      name: '/topic/share'
    }
  ];
  return oemTabs;
});
</script>

<style></style>
