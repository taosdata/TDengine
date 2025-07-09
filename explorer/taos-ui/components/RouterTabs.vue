<template>
  <el-tabs v-model="activeName" :before-leave="beforeLeave" @tab-click="tabClick">
    <el-tab-pane v-for="item in props.tabs" :key="item.path" :name="item.path" v-bind="item"></el-tab-pane>
  </el-tabs>
</template>

<script lang="ts" setup>
import { TabPaneName, TabsPaneContext } from 'element-plus';
import { useRoute, useRouter } from 'hooks/useCurrentRouter';
import { isRouteAborted } from 'utils/route';

const props = withDefaults(
  defineProps<{
    tabs: Array<any>;
    checkIgnored?: boolean;
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
  if (!props.tabs.every(item => item.path != route.path) || props.checkIgnored) {
    return;
  }
  if (props.tabs.length) {
    activeName.value = props.tabs[0].path as string;
    router.push({
      path: props.tabs[0].path,
      query: props.tabs[0].query || {}
    });
  } else {
    router.back();
  }
});
const beforeLeave = async (currentName: TabPaneName) => {
  try {
    if (route.path === currentName) {
      // if the current name equals route path, no need to push the router again
      return true;
    }
    const res = await router.push({
      path: (currentName as string) ?? '',
      query: props.tabs.find(tab => tab.path === currentName)?.query || {}
    });
    if (isRouteAborted(res)) {
      return false;
    } else {
      emit('tab-click', { paneName: currentName });
      return true;
    }
  } catch (error) {
    return false;
  }
};
const tabClick = (tab: TabsPaneContext) => {
  // need to hand
  emit('tab-click', { tab });
};
</script>

<style scoped lang="scss"></style>
