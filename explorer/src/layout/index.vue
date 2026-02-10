<template>
  <div class="layout-wrapper" :class="sider_style">
    <Sider class="sider"></Sider>
    <div class="main">
      <LayoutHeader :reload="reload"></LayoutHeader>
      <main class="main-content">
        <router-view v-if="isRouterAlive"></router-view>
      </main>
    </div>
    <div class="status-bar">
      <div class="status-left">
        <Version :statusBar="true" />
      </div>
      <div class="status-right">
        <International />
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { useStore } from 'vuex';
import { Sider, LayoutHeader } from './components/index';
import { useResizeHandler } from '@/hooks/useResizeHandler';
import Version from './components/Header/components/Version/index.vue';
import International from './components/Header/components/International/index.vue';

const store = useStore();
const { $_initResizeEvent, $_destroyResizeEvent } = useResizeHandler();
const isRouterAlive = ref<boolean>(true);

const sider_style = computed(() => {
  return store.state.sidebar.opened ? 'sider_unfold' : 'sider_fold';
});

const timezone = computed(() => {
  return store.state.app.timeZone;
});

watch(timezone, () => {
  reload();
});

function reload() {
  isRouterAlive.value = false;
  nextTick(() => {
    isRouterAlive.value = true;
  });
}

onMounted(() => {
  $_initResizeEvent();
});

onBeforeUnmount(() => {
  $_destroyResizeEvent();
});
</script>

<style scoped lang="scss">
.layout-wrapper {
  display: flex;
  flex-direction: row;
  height: 100%;
}

.sider {
  flex-shrink: 0;
  height: 100%;
}

.main {
  display: flex;
  flex: 1;
  flex-direction: column;
  overflow-x: auto;
  background-color: #f2f3f3;
}

.main-content {
  flex: 1;
  width: 100%;
  height: calc(100% - 58px);
  padding: 10px 15px 0px 10px;
  overflow-y: auto;
  background-color: #f2f3f3;
  margin-bottom: 40px; // 状态栏高度
}

.status-bar {
  position: fixed;
  bottom: 0;
  left: 0;
  right: 0;
  height: 40px;
  background-color: #f2f3f3;
  border-top: 1px solid #f2f3f3;
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 0 15px;
  z-index: 1000;

  .status-left :deep(.license span) {
    font-size: 14px;
    color: var(--el-text-color-secondary);
  }

  .status-right :deep(.language) {
    font-size: 13px;
    margin-top: 0px;
    margin-right: 0px;
    border-color: var(--el-text-color-secondary);
    color: var(--el-text-color-secondary);
  }
}

.status-left {
  flex: 1;
}

.status-right {
  flex-shrink: 0;
}
</style>
