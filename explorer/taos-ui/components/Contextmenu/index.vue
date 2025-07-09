<template>
  <div class="mask" @contextmenu.prevent="removeContextmenu()" @mousedown.left="removeContextmenu()"></div>

  <div
    class="contextmenu"
    :style="{
      left: style.left + 'px',
      top: style.top + 'px'
    }"
    @contextmenu.prevent
  >
    <MenuContent :menus="menus" :handle-click-menu-item="handleClickMenuItem" />
  </div>
</template>

<script lang="ts" setup>
import { computed } from 'vue';
import type { ContextmenuItem, Axis } from './types';

import MenuContent from './MenuContent.vue';

const props = defineProps<{
  axis: Axis;
  el: HTMLElement;
  menus: ContextmenuItem[];
  removeContextmenu: () => void;
}>();

const style = computed(() => {
  const MENU_WIDTH = 180;
  const MENU_HEIGHT = 30;
  const DIVIDER_HEIGHT = 11;
  const PADDING = 5;

  const { x, y } = props.axis;
  const menuCount = props.menus.filter(menu => !(menu.divider || menu.hide)).length;
  const dividerCount = props.menus.filter(menu => menu.divider).length;

  const menuWidth = MENU_WIDTH;
  const menuHeight = menuCount * MENU_HEIGHT + dividerCount * DIVIDER_HEIGHT + PADDING * 2;

  const screenWidth = document.body.clientWidth;
  const screenHeight = document.body.clientHeight;

  return {
    left: screenWidth <= x + menuWidth ? x - menuWidth : x,
    top: screenHeight <= y + menuHeight ? y - menuHeight : y
  };
});

const handleClickMenuItem = (item: ContextmenuItem) => {
  if (item.disable) return;
  if (item.children && !item.handler) return;
  if (item.handler) item.handler(props.el);
  props.removeContextmenu();
};
</script>

<style lang="scss">
.mask {
  position: fixed;
  top: 0;
  left: 0;
  z-index: 9998;
  width: 100vw;
  height: 100vh;
}

.contextmenu {
  position: fixed;
  z-index: 9999;
  user-select: none;
}
</style>
