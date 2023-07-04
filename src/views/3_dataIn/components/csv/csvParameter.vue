<template>
  <div class="csv-parameter">
    <el-form :model="ruleForm" ref="ruleForm" label-width="150px">
      <el-form-item label="包含Header：" prop="hasHeader">
        <el-checkbox v-model="ruleForm.hasHeader"></el-checkbox>
      </el-form-item>
      <el-form-item label="DB：" prop="dbName">
        <el-select v-model="ruleForm.dbName" placeholder="请选择数据源">
          <el-option label="区域一" value="shanghai"></el-option>
          <el-option label="区域二" value="beijing"></el-option>
        </el-select>
      </el-form-item>
      <el-form-item label="精度：" prop="percision"> 
         <el-select v-model="ruleForm.percision" placeholder="请选择精度">
          <el-option label="区域一" value="shanghai"></el-option>
          <el-option label="区域二" value="beijing"></el-option>
        </el-select>
      </el-form-item>
      <el-form-item label="Table：" prop="tableName">
        <el-input v-model="ruleForm.tableName"></el-input>
      </el-form-item>
    </el-form>
  </div>
</template>
<script>
export default {
  name: "CsvParameter",
  data() {
    return {
      ruleForm: {
        hasHeader: false,
        dbName: "",
        percision: "",
        tableName: "",
      },
    };
  },
};
</script>
<style lang="scss" scoped>
.csv-parameter{
    width:500px;
}
</style>
