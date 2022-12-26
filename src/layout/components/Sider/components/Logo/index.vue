<template>
  <div class="sidebar_logo_container">
    <router-link to="/landing">
      <img v-if="opened" src="./logo_expend.svg" class="sidebar_logo_expend" />
      <img v-else src="@/assets/logo.svg" class="sidebar_logo_fold" />
    </router-link>
  </div>
</template>

<script>
  import { mapState } from "vuex";

  export default {
    computed: mapState("sidebar", ["opened"]),
    data() {
      return {};
    },
  };
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
      width: 180px;
      height: 68px;
      transition-duration: 0.4s;
    }
    & .sidebar_logo_fold {
      width: 40px;
      height: 40px;
      margin-top: 9px;
    }
  }
</style>
