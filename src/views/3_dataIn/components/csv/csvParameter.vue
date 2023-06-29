<template>
  <div class="csv-parameter">
    <el-form :model="ruleForm" ref="ruleForm">
        <el-form-item label=""></el-form-item>
    </el-form>
  </div>
</template>
<script>
export default {
    name:'CsvParameter',
    data(){
        return{
            ruleForm:{
                hasHeader:false,
                dbName:'',
                percision:'',
                tableName:''
            }
        }
    }
}
</script>
