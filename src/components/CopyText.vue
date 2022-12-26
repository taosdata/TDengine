<template>
  <div class="copy-wrapper">
    {{ text }}
    <button circle class="copy-btn" :title="$t('copy')" @click="copy">
      <el-icon class="el-icon-document-copy"></el-icon>
      {{ btnText }}
    </button>
  </div>
</template>

<script>
  import { copy } from "@/utils";
  export default {
    props: {
      text: {
        type: String,
        default: "",
      },
      isShowBtnText: {
        type: Boolean,
        default: false,
      },
    },
    computed: {
      btnText() {
        return this.isShowBtnText ? this.$t("copy") : "";
      },
    },
    data() {
      return {};
    },
    watch: {},
    created() {},
    mounted() {},
    methods: {
      copy() {
        copy(this.text);
      },
    },
  };
</script>

<style scoped lang="scss">
  .copy-wrapper {
    position: relative;
    line-height: 40px;
    // font-size: 26px;
    @extend .nowrap;
    > .copy-btn {
      position: absolute;
      right: 0;
      top: 50%;
      font-size: 14px;
      transform: translateY(-50%);
      display: none;
      border: none;
      //   border: 1px solid #dcdfe6;
      border-radius: 2px;
      background: #fff;
      padding: 2px 5px;
      cursor: pointer;
    }
    &:hover {
      & > .copy-btn {
        display: block;
        color: $color-primary;
        // border-color: $color-primary;
      }
    }
  }
</style>
