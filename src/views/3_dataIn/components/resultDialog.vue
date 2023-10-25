<template>
  <el-dialog
    :visible.sync="resultVisible"
    width="500px"
    :show-close="false"
    title="提示"
  >
    <div class="text">
      <i :class="content.icon"></i>
      <span>{{ content.contentText  }}</span>
    </div>
    <span
      slot="footer"
      class="dialog-footer"
      v-if="JSON.stringify(result) !== '{}'"
    >
      <el-button type="primary" size="small" plain @click="handleClose"
        >{{ $t('confirm') }}</el-button
      >
    </span>
  </el-dialog>
</template>

<script>

export default {
  name: "ResultDialog",
  props: {
    result: {
      type: Object,
    },
    resultVisible: {
      type: Boolean,
      default: () => false,
    },
    loading: {
      type: Boolean,
      default: () => true,
    },
    percentage: {
      type: Number,
      default: () => 10,
    },
  },
  data() {
    return {
      colors: [
        { color: "#f56c6c", percentage: 20 },
        { color: "#e6a23c", percentage: 40 },
        { color: "#5cb87a", percentage: 60 },
        { color: "#1989fa", percentage: 80 },
        { color: "#6f7ad3", percentage: 100 },
      ],
    };
  },
  computed: {
    visible: {
      get() {
        return this.resultVisible;
      },
      set(val) {
        // this.resultVisible = val
      },
    },
    content() {
      let contentText = '';
      let icon = ''
      const { valid, support, version, message } = this.result;
      if (valid) {
        if (support) {
          contentText = version
            ? this.$t("dataIn.successVersionTip").replace("{version}", version)
            : this.$t("dataIn.successTip");
          icon = 'el-icon-success'
        } else {
          contentText = this.$t("dataIn.unSupportTip").replace(
            "{version}",
            version
          );
          icon = 'el-icon-error'
        }
      } else {
        contentText = this.$t("dataIn.failTip") + message;
        icon = 'el-icon-error'
      }
      console.log('tyy');
      return {contentText,icon};
    },
  },
  methods: {
    handleClose() {
      this.$emit("cancelModal");
    },
  },
  components: {},
};
</script>

<style scoped lang="scss">
::v-deep .el-progress.is-success .el-progress__text {
  color: #33b169 !important;
  font-size: 28px !important;
}
::v-deep .el-progress.is-exception .el-progress__text {
  color: #ff2e4d !important;
  font-size: 28px !important;
}
::v-deep .el-icon-check {
  font-weight: 600;
}

::v-deep .el-icon-close {
  font-weight: 600;
}

.text {
  display: flex;
  align-items: center;
  text-align: left;
  font-size: 16px;
  font-weight: 500;
  padding: 10px;
  >span{
    white-space: pre-wrap;
    word-break: break-all;
    word-wrap: break-word;
  }
}

.el-icon-error {
  color: #ff2e4d;
  font-size: 20px;
  margin-right: 10px;
}
.el-icon-success{
  color: #33b169;
  font-size: 20px;
  margin-right: 10px;
}

</style>
