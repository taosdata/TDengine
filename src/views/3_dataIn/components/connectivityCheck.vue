<template>
  <section>
    <el-collapse v-model="activeCollapse" accordion>
      <el-collapse-item name='one'>
        <template slot="title">
          <el-button
            :loading="checkLoading"
            type="primary"
            size="small"
            @click.capture.stop="clickCheckBtn"
            >{{ $t("dataIn.check") }}
          </el-button>
        </template>
        <Result
          v-show="JSON.stringify(checkResult) !== '{}'"
          :result="checkResult"
        /> 
      </el-collapse-item>
    </el-collapse>
  </section>
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
    }
  },
  methods: {
    clickCheckBtn() {
      this.checkResult = this.$options.data().checkResult;
      const errorMsg = [];
      const validFieldList = this.validFieldList.filter(item => document.querySelector(`.source-ui .left-ui .${getFieldClassMarkName(item)}`));
      this.sourceParent.$refs.form.validateField(validFieldList, valid => {
        errorMsg.push(valid);
        if (errorMsg.length == validFieldList.length && errorMsg.some(item => !item)) {
          this.activeCollapse = '';
          const type = this.sourceParent.tagName;
          const dsn = type + getDsnData(this.sourceParent.sourceForm.data, this.sourceParent.currentDefinition)
          this.getValidateResult(dsn,this.agentId);
        } else {
          this.$nextTick(() => {
            document.querySelector('.source-ui .left-ui .is-error')?.scrollIntoView();
          });
        }
      });
    },
    // 数据源可用性和版本检查
    async getValidateResult(dns,agentId) {
      try {
        this.checkLoading = true;
        let result = await validateTask(dns, agentId);
        this.checkResult = result;
        this.checkLoading = false; // 检测的 loading 效果
        this.activeCollapse = "one";
      } catch (error) {
        this.checkLoading = false;
        console.log("err");
      }
    },
    getValidFieldList(data, result, parent = 'data') {
      for (const val of data) {
        // if (val.field == 'checkConnectivity') break;
        if (val.field == 'mode') break;
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