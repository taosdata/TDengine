<template>
  <div class="csv-data">
    <div class="file-settings">
      <el-tabs v-model="activeName" @tab-click="handleClick">
        <el-tab-pane :label="$t('datasource.uploadcsv')" name="first">
          <div class="upload-file">
            <span class="label required">{{ $t("datasource.upfile") }}</span>
            <el-upload
              class="upload-demo"
              ref="upload"
              :data="uploadData"
              :action="uploadUrl"
              :on-preview="handlePreview"
              :on-remove="handleRemove"
              :on-success="handleSuccess"
              :file-list="fileList"
              :auto-upload="true"
            >
              <el-button slot="trigger" size="small" type="primary">{{
                $t("datasource.selectfile")
              }}</el-button>
            </el-upload>
          </div>
        </el-tab-pane>
        <el-tab-pane :label="$t('datasource.configcsv')" name="second">
          <div class="upload-file">
            <span class="label required">{{ $t("datasource.fileurl") }}</span>
            <el-input></el-input>
          </div>
        </el-tab-pane>
        <CsvParameter ref="param"></CsvParameter>
      </el-tabs>
      <el-button
        type="primary"
        @click="getCsvColumnsData"
        size="medium"
        class="nextbtn"
        >{{ $t("datasource.csvNext") }}</el-button
      >
    </div>
    <div class="csv-config" v-if="showConfig">
      <ul class="csv-tableheader">
        <li>{{ $t("datasource.csvcol") }}</li>
        <li>{{ $t("datasource.dbcol") }}</li>
        <li>{{ $t("datasource.coltype") }}</li>
        <li>{{ $t("datasource.primarykey") }}</li>
        <li>{{ $t("datasource.ascolumn") }}</li>
        <li>{{ $t("datasource.astag") }}</li>
      </ul>
      <ul v-for="(item, index) in csvColumns" :key="item">
        <li class="csv-content">
          <div class="csv-col">{{ item }}</div>
          <CsvColumn
            :csvColName="item"
            :key="index"
            :index="index"
            :colData="localcsv"
            :dbOptions="dbOptions"
            @changePrimary="changePrimary"
            :isEditable="isEditable"
            @changeAddStatus="changeAddStatus"
            @handleVisble="handleVisble"
            @handledbChange="handledbChange"
            @handleFilter="handleFilter"
            ref="mqtt"
          ></CsvColumn>
        </li>
      </ul>
    </div>
  </div>
</template>
<script>
import CsvParameter from "./csv/csvParameter.vue";
import CsvColumn from "./csv/csvColumn.vue";
import { deepClone } from "@/utils";
import { sendSQLReq } from "@/api/gateway/console";
import { getCSVColumns } from "@/api/explorer/datain";
export default {
  name: "CsvData",
  components: { CsvParameter, CsvColumn },
  props:{
    isEditable:{
      type:Boolean,
      default:false
    },
    echoData:{
      type:Object,
      default:()=>{
        return null
      }
    }
  },
  provide() {
    return {
      currentKey: this.currentKey,
    };
  },
  data() {
    return {
      showConfig:false,
      csvParserConf: {},
      uploadData: {
        req_id: new Date().getTime(),
      },
      dbValues: [],
      oldDbValues: [],
      currentKey: {
        primary: "",
      },
      activeName: "first",
      fileList: [],
      uploadUrl: process.env.VUE_APP_X_API + `/upload`,
      csvColumns: [],
      localcsv: {},
      dbOptions: [],
    };
  },
  mounted() {
    if(this.isEditable){
      //编辑状态直接从返回值去csv 的parser
    }
    this.getDBColumns();
  },
  methods: {
    //初始化options，csv列没有对应db列，则db列默认和csv列名称一样
    initDbOptions() {
      try {
        let result = Object.keys(this.localcsv.parser.parse);
        result.map((item) => {
          let alias = this.localcsv.parser.parse[item].alias
          this.dbOptions.push({
            disabled: false,
            field: alias,
            length: 8,
            note: "",
            type: "",
          });
        });
        console.log(this.dbOptions, "初始化方法");
      } catch (error) {
        console.log(error);
      }
    },
    handleVisble(visible, value) {
      if (!visible) {
        const item = this.dbOptions.find((item) => item.rewriting);
        if (!item) return;
        item.rewriting = false;
        if (value !== item.field) {
          this.dbOptions.splice(this.dbOptions.indexOf(item), 1);
        }
      }
      console.log(visible,value,'handleVisble');
    },
    handledbChange(value, index) {
      const oldItem = this.dbOptions.find(
        (item) => item.field === this.oldDbValues[index]
      );
      this.oldDbValues[index] = value;
      if (oldItem) {
        oldItem.disabled = false;
      }
      const item = this.dbOptions.find((item) => item.field === value);
      if (!item) return;
      item.disabled = true;

      console.log("change", value, this.dbOptions);
    },
    handleFilter(value) {
      const item = this.dbOptions.find((item) => item.rewriting);

      console.log(value, "filter---999", item);
      if (!value && !item) return true;
      if (!value && item) {
        this.dbOptions.splice(this.dbOptions.indexOf(item), 1);
        return true;
      }
      if (this.dbOptions.some((item) => item.field === value)) return true;
      if (item) {
        console.log(item, " item幼稚");
        // item.value = value;
        item.field = value;
        return true;
      } else {
        console.log("xinz新增push");
        this.dbOptions.push({
          // value,
          field: value,
          rewriting: true,
          newByInpt: true,
          disabled: false,
        });
      }
      console.log(this.dbOptions, "this.dbOptions");
      return true;
    },
    changePrimary() {},
    changeAddStatus() {},
    handleClick() {},
    handleSuccess(response, file, fileList) {
      this.fileList = fileList;
    },
    submitUpload() {
      this.$refs.upload.submit();
    },
    handleRemove(file, fileList) {
      console.log(file, fileList);
    },
    handlePreview(file) {
      console.log(file, "文件");
    },
    async getCsvColumnsData() {
      try {
        this.$refs.param.submit();
        console.log(this.$refs.param.isValid, this.fileList, "参数9999");
        if (this.$refs.param.isValid && this.fileList.length > 0) {
          let result = await getCSVColumns(
            this.fileList[0].response,
            "csv",
            this.$refs.param.ruleForm.hasHeader
          );
          this.csvParserConf = {
            parser: {
              parse: {},
              model: {
                name: "",
                using: "",
                tags: [],
                columns: [],
              },
            },
          };
          this.csvColumns = result.file_header.column_names;
          result.file_header.column_names.forEach((item) => {
            this.csvParserConf.parser.parse[item] = {
              as: "",
              alias: item,
            };
          });
          this.localcsv = deepClone(this.csvParserConf);
          this.$store.commit('app/SET_CSV_PARSER',this.localcsv)
          this.initDbOptions();
          this.showConfig=true
        }
      } catch (error) {
        console.log(error);
      }
      this.$refs.upload.submit();
    },
    async getDBColumns() {
      try {
        let result = await sendSQLReq(
          ` describe \`opc\`.\`meters_double_ffs-123\` ;`
        );

        let res = result.data.map((db) => {
          return Object.fromEntries(
            result.column_meta.map((item, index) => {
              return [item[0], db[index]];
            })
          );
        });
        this.dbOptions = this.dbOptions.concat(
          res.map((item) => {
            return Object.assign(item, { disabled: false });
          })
        );

        console.log(result, this.dbOptions, "获取db的列");
      } catch (error) {
        console.log(error);
      }
    },
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
    margin-bottom: 18px;
    align-items: center;
    .label {
      padding-right: 35px;
      color: #4d6992;
      width: 125px;
      font-weight: 500;
      font-size: 16px;
      text-align: right;
      position: relative;
      &.required {
        &::before {
          content: "*";
          color: red;
          font-size: 16px;
          line-height: 25px;
          right: 100px;
          position: absolute;
        }
      }
    }
    .el-input {
      flex: 1;
    }
  }
  .nextbtn {
    width: 370px;
    margin-left: 130px;
  }
  .csv-config {
    margin-top: 30px;
    .csv-tableheader {
      display: grid;
      grid-template-columns: 1fr 1.5fr 1.5fr 1fr 1fr 1fr;
      column-gap: 10px;
      background-color: #f5f7fa;
      border: 1px solid #ebeef5;
      border-bottom: none;
      padding-top: 5px;
      padding-bottom: 5px;
      li {
        display: flex;
        justify-content: center;
        color: #909399;
        font-size: 16px;
      }
    }
    .csv-content {
      display: grid;
      grid-template-columns: 1fr auto;
      .csv-col {
        width: 120px;
        display: flex;
        align-items: center;
        justify-content: center;
        &:first-child {
          border-top: 1px solid #ebeef5;
        }
      }
    }
  }
}
</style>
