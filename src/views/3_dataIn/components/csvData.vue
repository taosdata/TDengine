<template>
  <div class="csv-data">
    <div class="file-settings">
      <el-tabs v-model="activeName" @tab-click="handleClick">
        <el-tab-pane :label="$t('datasource.uploadcsv')" name="first">
          <div class="upload-file">
            <span :class="['label required',language.includes('en')?'en':'zh']">{{ $t("datasource.upfile") }}</span>
            <el-upload
              class="upload-demo"
              ref="upload"
              accept=".csv"
              :on-remove="handleRemove"
              :data="uploadData"
              :action="uploadUrl"
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
            <el-input v-model="fileurl"></el-input>
          </div>
        </el-tab-pane>
        <CsvParameter
          ref="param"
          :targetName="dbName"
          :echoData="echoData"
          :isEditable="isEditable"
        >
          <template v-slot:next>
            <!-- <span style="color:red;font-size:24px;">这是个插槽</span> -->
            <el-button
              type="primary"
              @click="getCsvColumnsData"
              size="medium"
              class="nextbtn"
              >{{ $t("datasource.csvNext") }}</el-button
            >
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
                  <div class="csv-col">
                    <el-tooltip
                      effect="light"
                      placement="right-end"
                      :content="item"
                    >
                      <span
                        style="
                          width: 120px;
                          overflow: hidden;
                          text-overflow: ellipsis;
                        "
                        >{{ item }}</span
                      ></el-tooltip
                    >
                  </div>

                  <CsvColumn
                    :csvColName="item"
                    :key="index"
                    :index="index"
                    :colData="localcsv"
                    :dbOptions="dbOptions"
                    :isEditable="isEditable"
                    @handleVisble="handleVisble"
                    @handledbChange="handledbChange"
                    @handleFilter="handleFilter"
                    @handleClear="handleClear"
                    ref="csvconfig"
                  ></CsvColumn>
                </li>
              </ul>
            </div>
          </template>
        </CsvParameter>
      </el-tabs>
    </div>
  </div>
</template>
<script>
import CsvParameter from "./csv/csvParameter.vue";
import CsvColumn from "./csv/csvColumn.vue";
import { deepClone } from "@/utils";
import { sendSQLReq } from "@/api/gateway/console";
import { getCSVColumns } from "@/api/explorer/datain";
import { Message } from "element-ui";
export default {
  name: "CsvData",
  components: { CsvParameter, CsvColumn },
  props: {
    isEditable: {
      type: Boolean,
      default: false,
    },
    echoData: {
      type: Array,
      default: () => {
        return [];
      },
    },
    dbName: {
      type: String,
      default: "",
    },
  },
  provide() {
    return {
      currentKey: this.currentKey,
    };
  },
  filter: {},
  data() {
    return {
      language:window.navigator.language,
      showConfig: false,
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
      fileurl: "",
      uploadUrl: process.env.VUE_APP_X_API + `/upload`,
      csvColumns: [],
      localcsv: {},
      dbOptions: [],
    };
  },
  mounted() {
    if (this.isEditable) {
      //编辑状态直接从返回值去csv 的parser
      this.activeName = "second";
      this.showConfig = true;
      this.fileList = this.$store.state.app.csvfiles
        .split(",")
        .map((item, index) => {
          return {
            name: item.substr(item.lastIndexOf("/") + 1),
            percentage: 100,
            raw: File,
            response: [].concat(item),
            size: 87,
            status: "success",
            uid: index,
          };
        });
      this.fileurl = this.fileList
        .map((item) => {
          return item.response[0];
        })
        .join("");
      this.echoEditData();
    }
  },
  methods: {
    handleRemove(file,filelist){
      this.fileList=filelist
    },
    //编辑状态的回显
    echoEditData() {
      this.csvColumns = Object.keys(this.echoData[0].parse);
      this.localcsv = deepClone({
        parser: this.echoData[0],
      });
      this.initDbOptions();
    },
    handleClear(index) {
      this.dbOptions.splice(index, 1);
    },
    //初始化options，csv列没有对应db列，则db列默认和csv列名称一样
    initDbOptions() {
      try {
        let result = Object.keys(this.localcsv.parser.parse);
        result.map((item) => {
          let alias = this.localcsv.parser.parse[item].alias;
          this.dbOptions.push({
            disabled: true,
            field: alias,
            length: 8,
            note: "",
            type: "",
          });
        });
      } catch (error) {
        console.log(error);
      }
    },
    handleVisble(visible, value) {
      if (!visible) {
        const disableItem = this.dbOptions.filter(
          (item) => item.field == value
        )[0];
        disableItem.disabled = true;
        const item = this.dbOptions.find((item) => item.rewriting);
        if (!item) return;
        item.rewriting = false;
        if (value !== item.field) {
          this.dbOptions.splice(this.dbOptions.indexOf(item), 1);
        }
      }
    },
    handledbChange(value, index) {
      const item = this.dbOptions.find((item) => item.field === value);
      if (!item) return;
      item.disabled = false;
    },
    handleFilter(value) {
      const item = this.dbOptions.find((item) => item.rewriting);
      if (!value && !item) return true;
      if (!value && item) {
        this.dbOptions.splice(this.dbOptions.indexOf(item), 1);
        return true;
      }
      if (this.dbOptions.some((item) => item.field === value)) return true;
      if (item) {
        item.field = value;
        return true;
      } else {
        this.dbOptions.push({
          field: value,
          rewriting: true,
          newByInpt: true,
          disabled: false,
        });
      }
      return true;
    },

    handleClick() {},
    handleSuccess(response, file, fileList) {
      this.fileList = fileList;
    },
    submitUpload() {
      this.$refs.upload.submit();
    },
   
    async getCsvColumnsData() {
      try {
        if (this.activeName == "first" && this.fileList.length == 0) {
          Message.error(this.$t("datasource.uploadcsvtip"));
          return;
        }
        if (this.activeName == "second" && !this.fileurl) {
          Message.error(this.$t("datasource.uploadcsvtip"));
          return;
        }
console.log('下一步',this.activeName,this.fileList);
        this.$refs.param.submit();
        
        if (this.isEditable) {
          this.$parent.$parent.isEditable = false;
          // this.isEditable=false
          console.log(this.$parent, "编辑状态");
        }
        console.log("请求接口获取csv列", this.activeName);
        // await this.getDBColumns();
        this.csvColumns = [];
        this.dbOptions = [];
        let result = null;
        if (this.activeName == "first") {
          if (this.$refs.param.isValid && this.fileList.length > 0) {
            if (this.$refs.param.ruleForm.hasHeader) {
              result = await getCSVColumns(
                this.fileList.map((item) => {
                  return item.response[0];
                }),
                "csv",
                this.$refs.param.ruleForm.hasHeader
              );
              this.csvColumns = result.file_header.column_names;
            } else {
              //无header需要自定义header
              this.csvColumns = this.$refs.param.ruleForm.customcol.split(",");
            }
          }
        } else {
          result = await getCSVColumns(
            this.fileurl,
            "csv",
            this.$refs.param.ruleForm.hasHeader
          );
          this.csvColumns = result.file_header.column_names;
        }
        // this.dbOptions = this.dbOptions.concat(
        //   this.csvColumns.map((item) => {
        //     return Object.assign(item, { disabled: true });
        //   })
        // );
        console.log(this.dbOptions, "有头无头的db列");
        if (result && result.message) {
          Message.error(result.message);
          return;
        }
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
        this.csvColumns.forEach((item) => {
          this.csvParserConf.parser.parse[item] = {
            as: "",
            alias: item,
          };
        });
        this.localcsv = deepClone(this.csvParserConf);
        this.$store.commit("app/SET_CSV_PARSER", this.localcsv.parser);
        this.initDbOptions();
        this.showConfig = true;
      } catch (error) {
        error && error.message && Message.error(error.message);
      }
      this.$refs.upload.submit();
    },
    async getDBColumns() {
      try {
        let result = await sendSQLReq(
          ` describe \`${this.$refs.param.ruleForm.dbName}\`.\`${this.$refs.param.ruleForm.tableName}\` ;`
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
            return Object.assign(item, { disabled: true });
          })
        );
      } catch (error) {
        console.log("表不存在则创建");
        // error&&error.desc&&Message.error(error.desc)
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
      padding-right: 40px;
      color: #4d6992;
      width: 225px;
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
          right: 110px;
          position: absolute;
        }
      }
    }
    .el-input {
      flex: 1;
    }
  }
  .nextbtn {
    width:100%;
    margin-top:20px;
    margin-bottom:20px;
  }
  .csv-config {
    margin-bottom:20px;
    .csv-tableheader {
      display: grid;
      grid-template-columns: 1fr 1.5fr 1.5fr 1fr 1fr 1fr;
      column-gap: 10px;
      background-color: #f5f7fa;
      // border: 1px solid #ebeef5;
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
      border-bottom: 1px solid #ebeef5;
      .csv-col {
        width: 123px;
        display: flex;
        align-items: center;
        justify-content: center;
        &:first-child {
          // border-top: 1px solid #ebeef5;
          padding-left: 10px;
        }
      }
    }
  }
}
</style>
