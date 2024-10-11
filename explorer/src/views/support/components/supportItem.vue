<template>
  <div class="support-item">
    <div class="status">
      <!-- 刚刚创建时 -->
      {{ support.state || $t("support.unProcessed") }}
    </div>
    <div class="title">
      {{ support.title }}
    </div>
    <div class="desc" v-text="desc"></div>
    <div class="time">
      {{ support.createTime }}
    </div>
  </div>
</template>

<script>
import * as marked from "marked";
export default {
  props: {
    support: {
      type: Object,
      default: function () {
        return {};
      },
    },
  },
  computed: {
    desc() {
      let result = "";
      try {
        result = marked.parse(this.support?.description);
      } catch (e) {
        console.log(e);
      }
      return result.replace(/<[^><]*>/gm, "");
    },
  },
};
</script>

<style lang="scss" scoped>
.support-item {
  display: flex;
  align-items: center;
  border-bottom: 1px solid #ebeef5;
  border-top: 1px solid #ebeef5;
  height: 40px;
  overflow: hidden;
  cursor: pointer;
  .title {
    width: 300px;
    @extend .nowrap;
    flex-shrink: 0;
    padding-right: 20px;
  }
  .id {
    width: 80px;
    @extend .nowrap;
    flex-shrink: 0;
    padding-right: 20px;
  }
  .status {
    width: 150px;
    @extend .nowrap;
    flex-shrink: 0;
    padding-right: 20px;
  }
  .time {
    text-align: right;
    width: 150px;
    @extend .nowrap;
    flex-shrink: 0;
  }
  .desc {
    flex: 1;
    @extend .nowrap;
    padding-right: 20px;
    font-weight: normal;
    color: #a0a0a0;
  }
  + .support-item {
    border-top: none;
  }
}
</style>
