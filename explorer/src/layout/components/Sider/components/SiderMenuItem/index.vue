<template>
  <div class="menu-item-wrap">
    <el-tooltip :disabled="opened" class="item" effect="dark" :content="$t(item.title)" placement="right">
      <el-menu-item
        :index="item.path"
        :disabled="!isDisabled()"
        class="menu-item"
        style="display: flex; align-items: center"
        @contextmenu.prevent="menuRight(item.path)"
      >
        <div :aria-data="item.path">
          <span>
            <Icon :name="item.icon" class="menu-item-icon" :class="{ 'menu-item-icon-unfold': opened }"></Icon>
          </span>
          <span v-if="opened" class="menu-item-title">
            {{ $t(item.title) }}
          </span>
        </div>
      </el-menu-item>
    </el-tooltip>
  </div>
</template>

<script setup lang="ts">
import { useStore } from 'vuex';
import { OpenNewTab } from '@/utils';
const store = useStore();

withDefaults(
  defineProps<{
    item: Record<string, any>;
  }>(),
  {
    item: () => ({})
  }
);
const opened = computed(() => store.state.sidebar.opened);

function isDisabled() {
  return true;
}
function menuRight(path: string) {
  OpenNewTab(path);
}

</script>

<style lang="scss" scoped>
.menu-item-wrap {
  margin-top: 10px;
}

.menu-item-icon {
  position: absolute;
  top: 50%;
  width: 24px;
  height: 24px;
  transform: translateY(-50%);
}

.menu-item-icon-unfold {
  left: 50px;
}

.menu-item-title {
  position: absolute;
  top: 50%;
  left: 90px;
  font-size: 16px;
  font-weight: 500;
  transform: translateY(-50%);
}

.menu-item {
  position: relative;
}

.el-menu-item {
  padding: 0 0 0 10px !important;
}
</style>
