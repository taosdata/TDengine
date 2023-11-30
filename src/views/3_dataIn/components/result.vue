<template>
  <div class="text">
    <i :class="content.icon"></i>
    <div class="flexWrap">
      <span :class="[lang]" v-if="content.contentText">{{ content.contentText  }}</span>
      <span :class="['error', lang]" v-if="content.messageText">{{content.messageText}}</span>
    </div>
  </div>
</template>

<script>
import { getBrowserLang } from '@/utils';
export default {
  name: "ResultDialog",
  props: {
    result: {
      type: Object,
    },
  },
  data() {
    return {};
  },
  computed: {
    content() {
      let contentText = '';
      let messageText = '';
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
        contentText = this.$t("dataIn.failTip");
        messageText = this.$t("dataIn.errorMessage") + message
        icon = 'el-icon-error'
      }
      return {contentText,messageText,icon};
    },
    lang() {
      return getBrowserLang() == 'zh' ? 'zh-text': 'en-text'
    }
  },
  methods: {},
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
  font-size: 14px;
  font-weight: 500;
  padding: 10px;
  color: #16191f;
  font-weight: 400;
}

.el-icon-error {
  color: #ff2e4d;
  font-size: 18px;
  margin-right: 10px;
}
.el-icon-success{
  color: #33b169;
  font-size: 18px;
  margin-right: 10px;
}
.flexWrap {
  display: flex;
  flex-wrap: wrap;
  >span{
    white-space: pre-wrap;
    word-wrap: break-word;
    display: inline-block;
    width: 100%;
  }
}
.zh-text {
  word-break: break-all;
}
.en-text {
  word-break: keep-all;
}
.error {
  color: red;
  margin-top: 8px;
}
</style>
