<template>
  <div class="sidebar_logo_container">
    <router-link :to="$IS_OEM?'/explorer':'/landing'">
      <span v-if="$IS_OEM" :class="opened ? 'oem' : 'oem-none'">{{ OEM_NAME }}</span>
      <template v-else>
        <img
          v-if="opened"
          src="./logo_expend.svg"
          class="sidebar_logo_expend"
        />
        <img v-else src="@/assets/icons/logo.svg" class="sidebar_logo_fold" />
      </template>
    </router-link>
  </div>
</template>

<script setup lang="ts">
import { useStore } from "vuex";
const store = useStore()
const { $IS_OEM, OEM_NAME } = inject("globalCustomProperties") as GlobalCustomProperties;
const opened = computed(() => store.state.sidebar.opened)
</script>

<style lang="scss" scoped>
.sidebar_logo_container {
  position: relative;
  width: 100%;
  height: 58px;
  text-align: center;
  border-bottom: 1px solid #eaecef;
  cursor: pointer;

  & .sidebar_logo_expend {
    width: 100%;
    height: auto;
    padding: 5% 5%;
    transition-duration: 0.4s;
  }
  & .sidebar_logo_fold {
    width: 40px;
    height: 40px;
    margin-top: 9px;
  }
  .oem{
    display: inline-block;
    font-size: 24px;
    margin-top: 15px;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .oem-none {
    visibility: hidden;
  }
}
</style>
