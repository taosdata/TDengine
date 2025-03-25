<template>
  <el-tabs v-model="activeName" @tab-click="tabClick">
    <template v-for="item in tabs" :key="item.name">
      <el-tab-pane v-bind="item" lazy></el-tab-pane>
    </template>
  </el-tabs>
</template>

<script setup lang="ts">
const router = useRouter();
const route = useRoute();
withDefaults(
  defineProps<{
    tabs: any[];
  }>(),
  {
    tabs: () => []
  }
);

const activeName = ref('');

watch(
  () => route,
  val => {
    activeName.value = val.path;
  },
  {
    deep: true,
    immediate: true
  }
);

function tabClick(tab) {
  router.push(tab.paneName);
}
</script>
<style scoped lang="scss"></style>
