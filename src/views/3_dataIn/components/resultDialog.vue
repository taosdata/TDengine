<template>
  <el-dialog
    :visible.sync="resultVisible"
    width="500px"
    :show-close="false"
    title="数据源连通性及版本检测"
  >
    <p class="text">
      {{ content }}
    </p>
    <span
      slot="footer"
      class="dialog-footer"
      v-if="JSON.stringify(result) !== '{}'"
    >
      <el-button type="primary" size="small" plain @click="handleClose"
        >确 定</el-button
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
      let contentText = "";
      const { valid, support, version, message } = this.result;
      if (valid) {
        if (support) {
          contentText = version
            ? this.$t("dataIn.successVersionTip").replace("{version}", version)
            : this.$t("dataIn.successTip");
        } else {
          contentText = this.$t("dataIn.unSupportTip").replace(
            "{version}",
            version
          );
        }
      } else {
        contentText = this.$t("dataIn.failTip") + message;
      }
      return contentText;
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

<style scoped>
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
  text-align: left;
  font-size: 16px;
  font-weight: 500;
  padding: 10px;
  white-space: pre-wrap;
  word-break: break-all;
  word-wrap: break-word;
}

.text-success {
  color: #33b169;
  padding-bottom: 10px;
}
.text-error {
  color: #ff2e4d;
  padding-bottom: 10px;
}
.text-content {
  padding-left: 20px;
}
</style>
