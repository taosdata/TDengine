<template>
  <div>
    <panelHeader @click.native="open = !open">
      <template slot="title">
        <span style="cursor: 'pointer'">{{ $t("calculator.assumptions") }}</span
        ><el-icon class="el-icon-caret-bottom icon-arrow-down" :class="{ open: open }"></el-icon>
      </template>
    </panelHeader>
    <section v-show="open" class="assumption-content">
      <div v-html="$t('calculator.assumptionsTip')"></div>
      <h3 class="tip2">
        {{ $t("for") }}
        <el-dropdown>
          <span class="el-dropdown-link"> 下拉菜单<i class="el-icon-arrow-down el-icon--right"></i> </span>
          <el-dropdown-menu slot="dropdown">
            <el-dropdown-item>黄金糕</el-dropdown-item>
            <el-dropdown-item>狮子头</el-dropdown-item>
            <el-dropdown-item>螺蛳粉</el-dropdown-item>
            <el-dropdown-item disabled>双皮奶</el-dropdown-item>
            <el-dropdown-item divided>蚵仔煎</el-dropdown-item>
          </el-dropdown-menu>
        </el-dropdown>
        <span v-html="tip2Text"></span>
      </h3>
    </section>
  </div>
</template>

<script>
  import panelHeader from "@/components/panelHeader.vue";
  export default {
    components: { panelHeader },
    data() {
      return {
        open: false,
      };
    },
    computed: {
      tip2Text() {
        return this.$t("calculator.assumptionsTip2");
      },
    },
  };
</script>

<style lang="scss" scoped>
  .icon-arrow-down {
    margin-left: 10px;
    transition: transform 0.3s;
  }
  .open {
    transform: rotate(180deg);
  }
  .assumption-content {
    margin-top: 20px;
    padding: 0 20px;
    max-width: 700px;
    &:deep(ol) {
      padding-left: 20px;
      list-style-position: outside;
      font-style: italic;
    }
    .tip2 {
      margin-top: 20px;
      font-weight: normal;
      font-size: 16px;
    }
  }
</style>
