<template>
  <div id="app">
    <router-view :key="key"></router-view>
    <el-dialog v-model:visible="dialogVisible" :close-on-click-modal="false" v-bind="dialogConfig">
      <component
        :is="dialogComponent"
        v-bind="dialogParams"
        v-on="dialogListener"
        @close="dialogVisible = false"
      ></component>
    </el-dialog>
    <systemMes v-if="!$IS_TSDBLITE && $IS_COMMUNITY && showSystemMes" />
  </div>
</template>

<script setup lang="ts">
import { useStore } from 'vuex';
import { useRouter } from 'vue-router';
const router = useRouter();
import systemMes from './components/communityMes.vue';
const store = useStore();
const { $IS_COMMUNITY, $IS_TSDBLITE, $IS_OEM } = inject('globalCustomProperties') as GlobalCustomProperties;

const key = computed(() => {
  return store.state.app.current_cluster?.id || '';
});
const showSystemMes = computed(() => store.state.app.showSystemMes);
const dialogConfig = computed(() => store.state.dialogConfig);
const dialogParams = computed(() => store.state.dialogParams);
const dialogListener = computed(() => store.state.dialogListeners);
const dialogComponent = computed(() => store.state.dialogComponent);
const dialogVisible = computed({
  get() {
    return store.state.dialogVisible;
  },
  set(val) {
    store.commit('SET_DIALOG_VISIBLE', val);
  }
});
const isRouteLoading = ref(true);

onMounted(async () => {
  if ($IS_OEM) {
    //是oem需要单独处理
    const link = document.querySelector("link[rel*='icon']") || document.createElement('link');
    const title = document.querySelector('title') as HTMLElement;
    title.innerText = import.meta.env.VITE_APP_CUS_NAME;
    link.remove();
  }
});
</script>

<style lang="scss" scoped>
#app :deep(.CodeMirror-placeholder) {
  color: #c0c4cc;
}
</style>
<style lang="scss">
.el-table th.el-table__cell > .cell {
  white-space: nowrap;
}
</style>
