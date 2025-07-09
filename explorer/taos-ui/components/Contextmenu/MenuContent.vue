<template>
  <ul class="menu-content">
    <template v-for="(menu, index) in menus" :key="menu.text || index">
      <li
        v-if="!menu.hide"
        class="menu-item"
        :class="{ divider: menu.divider, disable: menu.disable }"
        @click.stop="handleClickMenuItem(menu)"
      >
        <div
          v-if="!menu.divider"
          class="menu-item-content"
          :class="{
            'has-children': menu.children,
            'has-handler': menu.handler
          }"
        >
          <span class="text">{{ menu.text }}</span>
          <span v-if="menu.subText && !menu.children" class="sub-text">{{ menu.subText }}</span>

          <menu-content
            v-if="menu.children && menu.children.length"
            class="sub-menu"
            :menus="menu.children"
            :handle-click-menu-item="handleClickMenuItem"
          />
        </div>
      </li>
    </template>
  </ul>
</template>

<script lang="ts" setup>
import type { ContextmenuItem } from './types';

defineProps<{
  menus: ContextmenuItem[];
  handleClickMenuItem: (item: ContextmenuItem) => void;
}>();
</script>

<style lang="scss" scoped>
$menu-width: 180px;
$menu-height: 30px;
$sub-menu-width: 120px;

.menu-content {
  width: $menu-width;
  padding: 5px 0;
  margin: 0;
  list-style: none;
  background: #fff;
  border: 1px solid $borderColor;
  border-radius: $borderRadius;
  box-shadow: $boxShadow;
}

.menu-item {
  height: $menu-height;
  padding: 0 20px;
  font-size: 12px;
  line-height: $menu-height;
  color: #555;
  white-space: nowrap;
  cursor: pointer;
  background-color: #fff;
  transition: all $transitionDelayFast;

  &:not(.disable):hover > .menu-item-content > .sub-menu {
    display: block;
  }

  &:not(.disable):hover > .has-children.has-handler::after {
    transform: scale(1);
  }

  &:hover:not(.disable) {
    background-color: rgba($color: $themeColor, $alpha: 20%);
  }

  &.divider {
    height: 1px;
    padding: 0;
    margin: 5px;
    overflow: hidden;
    line-height: 0;
    background-color: #e5e5e5;
  }

  &.disable {
    color: #b1b1b1;
    cursor: no-drop;
  }
}

.menu-item-content {
  position: relative;
  display: flex;
  align-items: center;
  justify-content: space-between;

  &.has-children::before {
    position: absolute;
    top: 50%;
    right: 0;
    display: inline-block;
    width: 8px;
    height: 8px;
    content: '';
    border-color: #666 #666 transparent transparent;
    border-style: solid;
    border-width: 1px;
    transform: translateY(-50%) rotate(45deg);
  }

  &.has-children.has-handler::after {
    position: absolute;
    top: 3px;
    right: 18px;
    display: inline-block;
    width: 1px;
    height: 24px;
    content: '';
    background-color: rgba($color: #fff, $alpha: 30%);
    transition: transform $transitionDelay;
    transform: scale(0);
  }

  .sub-text {
    opacity: 0.6;
  }

  /* stylelint-disable-next-line no-descending-specificity */
  .sub-menu {
    position: absolute;
    top: -6px;
    left: 112%;
    display: none;
    width: $sub-menu-width;
  }
}
</style>
