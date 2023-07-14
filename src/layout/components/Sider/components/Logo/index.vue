<template>
  <div class="sidebar_logo_container">
    <router-link :to="title?'/explorer':'/landing'">
      <span v-if="title" class="oem">{{ title }}</span>
      <template v-else>
        <img
          v-if="opened"
          src="./logo_expend.svg"
          class="sidebar_logo_expend"
        />
        <img v-else src="@/assets/logo.svg" class="sidebar_logo_fold" />
      </template>
    </router-link>
  </div>
</template>

<script>
import { mapState } from "vuex";

export default {
  computed: mapState("sidebar", ["opened"]),
  data() {
    return {
      title: process.env.VUE_APP_CUS_NAME,
    };
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
  .oem{
    display: inline-block;
    font-size: 24px;
    margin-top: 15px;
    overflow: hidden;
    text-overflow: ellipsis;
  }
}
</style>
