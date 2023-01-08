<template>
  <div class="header">
    <div class="headerLeft">
      <!-- <ClusterSelector></ClusterSelector> -->
    </div>
    <div class="headerRight">

      <Support></Support>
      <Document></Document>
      <!-- <Github></Github> -->
      <!-- <International></International> -->

      <!-- <el-tooltip class="item" effect="light" :content="$t('route.alerts')" placement="top-start" v-if="hasAlert">
        <router-link class="header-item" to="/alert" :class="{ alert: alerts }">
          <Icon name="alert" class="avatar_svg"></Icon>
        </router-link>
      </el-tooltip>
      <Help></Help> -->
      <Avatar></Avatar>
    </div>
  </div>
</template>

<script>

  import { Avatar, ClusterSelector, Help,Support,Document } from "./components";
  export default {
    components: { Avatar, ClusterSelector, Help,Support,Document },
    data() {
      return {
        issueTypeList: [],
      };
    },
    computed: {
      alerts() {
        return this.$store.state.app.newAlert.length;
      },
      hasAlert() {
        return this.$store.getters.role == "1";
      },
    },
    methods: {},
  };
</script>

<style lang="scss" scoped>
  .header {
    display: flex;
    flex-direction: row;
    justify-content: space-between;
    align-items: center;
    background-color: #fff;
    padding-right: 40px;
    padding-left: 40px;
    position: sticky;
    top: 0;
    z-index: 1;
    height: 58px;
    width: 100%;
    border-bottom: 1px solid #eaecef;
    flex-shrink: 0;
  }

  .avatar_svg {
    width: 80%;
    height: 80%;
  }
  .header-item {
    margin-top: 4px;
    margin-right: 20px;
    border-radius: 50%;
    width: 25px;
    height: 25px;
    border: 1px solid $color-primary;
    color: $color-primary;
    @extend .flexCenter;
    cursor: pointer;
  }
  .alert {
    position: relative;
  }
  .alert::before {
    content: "";
    position: absolute;
    bottom: 5px;
    right: 0;
    width: 10px;
    height: 10px;
    border-radius: 50%;
    background-color: $color-danger;
    animation: blink 1.5s linear infinite;
  }
  @keyframes blink {
    0% {
      opacity: 0;
    }
    50% {
      opacity: 1;
    }
    100% {
      opacity: 0;
    }
  }
  .headerRight {
    display: flex;
    flex-direction: row;
    align-items: center;
  }
</style>
