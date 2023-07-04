<template>
  <div class="csv-data">
    <el-tabs v-model="activeName" @tab-click="handleClick">
      <el-tab-pane label="上传CSV文件" name="first">
        <div class="upload-file">
          <span class="label">文件：</span>
          <el-upload
            class="upload-demo"
            ref="upload"
            action="https://jsonplaceholder.typicode.com/posts/"
            :on-preview="handlePreview"
            :on-remove="handleRemove"
            :file-list="fileList"
            :auto-upload="false"
          >
            <el-button slot="trigger" size="small" type="primary"
              >选取文件</el-button
            >
          </el-upload>
        </div>
      </el-tab-pane>
      <el-tab-pane label="配置Taosx地址" name="second">
        <div class="upload-file">
          <span class="label">文件：</span>
          <el-input></el-input>
        </div>
      </el-tab-pane>
      <CsvParameter></CsvParameter>
    </el-tabs>
  </div>
</template>
<script>
import CsvParameter from "./csv/csvParameter.vue";
export default {
  name: "CsvData",
  components: { CsvParameter },
  data() {
    return {
      activeName: "first",
      fileList: [
        {
          name: "food.jpeg",
          url: "https://fuss10.elemecdn.com/3/63/4e7f3a15429bfda99bce42a18cdd1jpeg.jpeg?imageMogr2/thumbnail/360x360/format/webp/quality/100",
        },
        {
          name: "food2.jpeg",
          url: "https://fuss10.elemecdn.com/3/63/4e7f3a15429bfda99bce42a18cdd1jpeg.jpeg?imageMogr2/thumbnail/360x360/format/webp/quality/100",
        },
      ],
    };
  },
  methods: {
    handleClick() {},
    submitUpload() {
      this.$refs.upload.submit();
    },
    handleRemove(file, fileList) {
      console.log(file, fileList);
    },
    handlePreview(file) {
      console.log(file);
    },
  },
};
</script>
<style lang="scss" scoped>
.csv-data {
  border: 1px solid #dfe4ed;
  width:600px;
  padding: 20px;
  box-sizing: border-box;
  .upload-file {
    display: flex;
    .label{
        color:#4d6992;
        width:150px;
        font-weight: 500;
        font-size: 16px;
        text-align: right;
    }
    .el-input{
        flex: 1;
    }
  }
}
</style>
