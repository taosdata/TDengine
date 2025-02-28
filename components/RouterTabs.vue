<template>
  <el-tabs v-model="activeName" @tab-click="tabClick">
    <el-tab-pane v-for="item in props.tabs" :key="item.path" :name="item.path" v-bind="item"></el-tab-pane>
  </el-tabs>
</template>

<script lang="ts" setup>
import { TabsPaneContext } from 'element-plus';
import { useRoute, useRouter } from 'hooks/useCurrentRouter';
const props = withDefaults(
  defineProps<{
    tabs: Array<any>;
  }>(),
  {
    tabs: () => []
  }
);
const emit = defineEmits(['tab-click']);
const route = useRoute();
const router = useRouter();
const activeName = ref('');
watchEffect(() => {
  activeName.value = (route?.path as string) ?? '';
});
onMounted(() => {
  if (props.tabs.every(item => item.path != route.path)) {
    if (props.tabs.length) {
      activeName.value = props.tabs[0].path as string;
      router.push({
        path: props.tabs[0].path,
        query: props.tabs[0].query || {}
      });
    } else {
      router.back();
    }
  }
});
const tabClick = (tab: TabsPaneContext) => {
  router.push({
    path: (tab.paneName as string) ?? '',
    query: props.tabs?.[Number(tab.index!)]?.query || {}
  });
  emit('tab-click', tab);
};
</script>

<style scoped lang="scss"></style>
