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
export default {
  name: 'connectivityCheck',
  components: { Result },
  prop: {},
  inject: ['sourceParent'],
  computed: {
    clickCheckBtn() {
      return this.sourceParent.clickCheckBtn
    },
    activeCollapse() {
      return this.sourceParent.activeCollapse
    },
    checkResult() {
      return this.sourceParent.checkResult
    },
    checkLoading() {
      return this.sourceParent.checkLoading
    }
  }
}

</script>