<template>
  <div class="csv-data">
    <div class="file-settings">
      <el-tabs v-model="activeName" @tab-click="handleClick">
        <el-tab-pane label="上传CSV文件" name="first">
          <div class="upload-file">
            <span class="label">文件：</span>
            <el-upload
              class="upload-demo"
              ref="upload"
              :action="uploadUrl"
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
      <el-button type='primary' @click="getCsvColumns">Next</el-button>
    </div>
    <div class="csv-config">
      <ul v-for="item in csvColumns" :key="item">
        <li>
          <div>{{item}}</div>
          <CsvColumn 
          :key="index"
          :index="index"
          :colData="localcsv"
          @changePrimary="changePrimary"
          :isEditable="isEditable"
          @changeAddStatus="changeAddStatus"
          ref="mqtt"
          ></CsvColumn>
        </li>
      </ul>
    </div>
  </div>
</template>
<script>
import CsvParameter from "./csv/csvParameter.vue";
import CsvColumn from './csv/csvColumn.vue';
import csvParser from './csvparser.json'
import { deepClone } from "@/utils";
export default {
  name: "CsvData",
  components: { CsvParameter ,CsvColumn},
  provide() {
    return {
      currentKey: this.currentKey,
    };
  },
  data() {
    return {
       currentKey: {
        primary: "ts",
      },
      activeName: "first",
      fileList: [],
      uploadUrl:process.env.VUE_APP_X_API+`/upload`,
      csvColumns:['col_1','col_2','col_3','col_4','col_5'],
      localcsv:deepClone(csvParser)
    };
  },
  methods: {
    changePrimary(){},
    changeAddStatus(){},
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
    getCsvColumns(){
      this.$refs.upload.submit()
    }
  },
};
</script>
<style lang="scss" scoped>
.csv-data {
  // width: 600px;
  padding: 20px;
  box-sizing: border-box;
  .upload-file {
    display: flex;
    .label {
      color: #4d6992;
      width: 150px;
      font-weight: 500;
      font-size: 16px;
      text-align: right;
    }
    .el-input {
      flex: 1;
    }
  }
}
</style>
