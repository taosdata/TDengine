<template>
  <el-dialog
    align="center"
    :title="$t('datasource.addsource')"
    width="400px"
    :visible.sync="visible"
    :destroy-on-close="true"
  >
    <div class="connector">
      <div class="json-zone">
        <ul class="header">
            <li>列名</li>
            <li>重命名</li>
            <li>类型</li>
        </ul>
      </div>

      <el-form>
        <el-form-item label="子表名称："></el-form-item>
        <el-form-item label="超级表："></el-form-item>
        <el-form-item label="tags："></el-form-item>
      </el-form>
    </div>
  </el-dialog>
</template>
<script>
export default {
  name: "MqttConnector",
  data() {
    return {
      visible: true,

      ruleForm: {
        subtableName: "",
        supertableName: "",
        tagsName: "",
      },
    };
  },
};
</script>
<style lang="scss" scoped>
.json-zone{
    display: flex;
    flex-direction: column;
    .header{
        display: grid;
        grid-template-columns: 1fr 1fr 1fr;
    }
}
</style>