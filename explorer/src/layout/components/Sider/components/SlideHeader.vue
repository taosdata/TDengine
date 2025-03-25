<template>
  <section class="slide-header" :class="sider_style">
    <Logo></Logo>
    <MenuTrigger :class="['menu-trigger', $IS_OEM ? 'oem' : '']"></MenuTrigger>
  </section>
</template>

<script setup lang="ts">
import { useStore } from 'vuex';
import { Logo, MenuTrigger } from './index';

const { $IS_OEM } = inject('globalCustomProperties') as GlobalCustomProperties;
const store = useStore();
const sider_style = computed(() => {
  return store.state.sidebar.opened ? 'sider-unfold' : 'sider-fold';
});
</script>

<style scoped lang="scss">
.slide-header {
  position: relative;
  z-index: 2;
  flex-shrink: 0;
  background-color: #fff;
  transition: width 0.4s ease 0s;
}

.menu-trigger {
  position: absolute;
  top: 18px;
  right: -10px;
}

.menu-trigger.oem {
  top: 60px;
}

.sider-fold {
  width: 60px;

  :deep(.oem) {
    max-width: 60px;
  }
}

.sider-unfold {
  width: 240px;

  :deep(.oem) {
    max-width: 200px;
  }
}
</style>
