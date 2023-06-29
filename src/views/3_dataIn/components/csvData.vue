<template>
  <div class="csv-data">
    <el-tabs v-model="activeName" @tab-click="handleClick">
      <el-tab-pane label="上传CSV文件" name="first">上传CSV文件</el-tab-pane>
      <el-tab-pane label="配置Taosx地址" name="second">配置Taosx地址</el-tab-pane>
    </el-tabs>
  </div>
</template>
<script>
export default {
  name: "CsvData",
  data() {
    return {
      activeName: "first",
    };
  },
  methods:{
    handleClick(){}
  }
};
</script>
