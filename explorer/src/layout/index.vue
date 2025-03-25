<template>
  <div class="layout-wrapper" :class="sider_style">
    <Sider class="sider"></Sider>
    <div class="main">
      <LayoutHeader :reload="reload"></LayoutHeader>
      <main class="main-content">
        <router-view v-if="isRouterAlive"></router-view>
      </main>
    </div>
  </div>
</template>

<script setup lang="ts">
import { useStore } from 'vuex';
import { Sider, LayoutHeader } from './components/index';
import { useResizeHandler } from '@/hooks/useResizeHandler';

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
}

.main-content {
  flex: 1;
  width: 100%;
  min-height: calc(100% - 58px);
  padding: 15px;
  overflow-y: auto;
  background-color: #f2f3f3;
}
</style>
