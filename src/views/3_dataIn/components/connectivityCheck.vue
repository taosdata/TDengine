<template>
<div class="box-check-connectivity">
  <el-tooltip
    placement="top" effect="light" :open-delay="0" :disabled="!$COMMUNITY"
  >
    <template slot="content">
      <span v-html="$t('communityTip')"></span>
    </template>
    <el-button
      :loading="checkLoading"
      :disabled="$COMMUNITY"
      class="btn-check-connectivity"
      type="primary"
      size="small"
      plain
      @click.capture.stop="clickCheckBtn"
      >{{ $t("dataIn.check") }}
    </el-button>
  </el-tooltip>
  <Result
    v-show="JSON.stringify(checkResult) !== '{}'"
    :result="checkResult"
  /> 
</div>
</template>
<script>
import Result from "./result.vue";
import { getDsnData, getFieldClassMarkName } from "../utils";
import { validateTask } from "@/api/explorer/datain";
export default {
  name: 'connectivityCheck',
  components: { Result },
  prop: {},
  inject: ['sourceParent'],
  data() {
    return {
      activeCollapse: '',
      checkResult: {},
      checkLoading: false
    }
  },
  computed: {
    validFieldList() {
      const result = [];
      this.getValidFieldList(this.sourceParent.currentDefinition.config, result);
      return result;
    },
    type() {
      return this.sourceParent.sourceForm.type
    },
    isEdit() {
      return this.sourceParent.isEditable;
    },
    url() {
      return this.$i18n.locale.includes('en') ? "https://tdengine.com/enterprise/?utm_source=oss+&utm_medium=user&utm_campaign=explorer" : "https://www.taosdata.com/tdengine-enterprise?utm_source=oss+&utm_medium=user&utm_campaign=explorer";
    },
  },
  watch: {
    type(){
      this.checkResult = {}
    }
  },
  mounted() {
    if (this.isEdit) {
      this.clickCheckBtn()
    }
  },
  methods: {
    clickCheckBtn() {
      this.checkResult = this.$options.data().checkResult;
      const errorMsg = [];
      const validFieldList = this.validFieldList.filter(item => document.querySelector(`.source-ui .left-ui .${getFieldClassMarkName(item)}`));
      this.sourceParent.$refs.form.validateField(validFieldList, valid => {
        errorMsg.push(valid);
        if (errorMsg.length == validFieldList.length && errorMsg.every(item => !item)) {
          this.activeCollapse = '';
          const type = this.sourceParent.sourceForm.type
          const agent = this.sourceParent.sourceForm.agent
          const dsn = getDsnData(this.sourceParent.sourceForm.data, this.sourceParent.currentDefinition)
          const param = type === "tmq" ? dsn : type + dsn
          this.getValidateResult(param, agent);
        } else {
          this.$nextTick(() => {
            document.querySelector('.source-ui .left-ui .is-error')?.scrollIntoView();
          });
        }
      });
    },
    // 数据源可用性和版本检查
    async getValidateResult(dsn, agent) {
      try {
        this.checkLoading = true;
        let result = await validateTask(dsn, agent);
        this.checkResult = result;
        // opc 需要获取 namespace
        this.$store.commit('app/SET_CONNECTIVITY_CHECKRESULT',result)
        this.checkLoading = false; // 检测的 loading 效果
        this.activeCollapse = "one";
      } catch (error) {
        this.checkLoading = false;
        console.log("err");
      }
    },
    getValidFieldList(data, result, parent = 'data') {
      for (const val of data) {
        if (val.field == 'checkConnectivity') break;
        if (val.children) {
          this.getValidFieldList(val.children, result, parent + '.' + val.field);
        } else {
          if (val.required) {
            result.push(parent + '.' + val.field);
          }
        }
      }
    },
  }
}

</script>
<style lang="scss" scoped>
  .connection {
    border-top: 0;
    border-bottom: 0;
    ::v-deep .el-collapse-item__header {
      border-bottom: 0;
    }
    ::v-deep .el-collapse-item__wrap {
      border-bottom: 0;
    } 
    :deep(.el-collapse-item__content) {
      padding-bottom: 0,
    }
  }
  .box-check-connectivity {
    margin-bottom: 30px;
    .btn-check-connectivity {
      width: 100%;
    }
  }
</style>