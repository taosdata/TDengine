<template>
  <div class="csv-data">
    <div class="file-settings">
      <el-tabs v-model="activeName" @tab-click="handleClick">
        <el-tab-pane :label="$t('datasource.uploadcsv')" name="first">
          <div class="upload-file">
            <span
              :class="['label required', language.includes('zh') ? 'zh' : 'en']"
              >{{ $t("datasource.upfile") }}</span
            >
            <el-tooltip
              placement="top" effect="light" :open-delay="0" :disabled="!$COMMUNITY"
            >
              <template slot="content">
                <span v-html="$t('communityTip')"></span>
              </template>
              <el-upload
                class="upload-demo"
                ref="upload"
                accept=".csv"
                :on-remove="handleRemove"
                :data="uploadData"
                :action="uploadUrl"
                :on-success="handleSuccess"
                :before-upload="checkFileName"
                :file-list="fileList"
                :auto-upload="true"
                size="small"
              >
                <el-button slot="trigger" size="small" type="primary" plain :disabled="$COMMUNITY">{{
                  $t("datasource.selectfile")
                }}</el-button>
              </el-upload>
            </el-tooltip>
            <span
              style="color: red; font-size: 12px; margin-left: 10px"
              v-if="showfiletip"
              >{{ this.$t("datasource.uploadcsvtip") }}</span
            >
          </div>
        </el-tab-pane>
        <el-tab-pane :label="$t('datasource.configcsv')" name="second">
          <el-form
            :model="fileForm"
            ref="fileform"
            :rules="fileRules"
            label-width="220px"
          >
            <el-form-item prop="fileurl">
              <template slot="label">
                <el-tooltip placement="top" effect="light" :open-delay="0">
                  <template slot="content">
                    <DocsContent
                      :content="$t('datasource.csvFileDesc')+$t('datasource.supportCharacter')"
                    />
                  </template>
                  <span>
                    <span>{{ $t('datasource.fileurl') }}</span>
                    <span style="margin-left: 4px">
                      <i class="el-icon-info"></i>
                    </span>
                  </span>
                </el-tooltip>
              </template>
              <el-input size="small" v-model="fileForm.fileurl"></el-input>
            </el-form-item>
          </el-form>
        </el-tab-pane>
        <CsvParameter ref="param" :echoData="echoData" :isEditable="isEditable">
          <template v-slot:next>
            <el-tooltip
              placement="top" effect="light" :open-delay="0" :disabled="!$COMMUNITY"
            >
              <template slot="content">
                <span v-html="$t('communityTip')"></span>
              </template>
              <el-button
                type="primary"
                @click="getCsvColumnsData"
                size="small"
                class="nextbtn"
                :loading="loading"
                :disabled="$COMMUNITY"
                >{{ $t("datasource.csvNext") }}</el-button
              >
            </el-tooltip>
            <CommonTransformer
              ref="transform"
              :parserColumns="extractArr"
              v-if="showTransformer"
            ></CommonTransformer>
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
import { getDsnData, getFieldClassMarkName } from "../utils";
import { sendSQLReq } from "@/api/gateway/console";
import { getCSVColumns } from "@/api/explorer/datain";
import { Message } from "element-ui";
import CommonTransformer from "./commonTransformer.vue";
import DocsContent from "@/views/support/components/editorContentDisplay.vue";
export default {
  name: "CsvData",
  components: { CsvParameter, CsvColumn, CommonTransformer, DocsContent },
  inject: ['sourceParent'],
  props: {
    isEditable: {
      type: Boolean,
      default: false,
    },
    isViewable: {
      type: Boolean,
      default: false,
    },
    echoData: {
      type: Array,
      default: () => {
        return [];
      },
    },
  },
  provide() {
    return {
      currentKey: this.currentKey,
    };
  },
  computed: {
    validFieldList() {
      const result = [];
      this.getValidFieldList(this.sourceParent.currentDefinition.config, result);
      return result;
    },
  },
  filter: {},
  data() {
    return {
      showfiletip: false,
      maptypes: ["value", "generator", "join", "format", "sum", "expr"],
      showTransformer: false,
      transformerParser: null,
      language: localStorage.getItem("local_language"),
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
      fileForm: {
        fileurl: "",
      },
      fileRules: {
        fileurl: [
          {
            required: true,
            // trigger: "blur",
            message: this.$t("datasource.uploadcsvtip"),
          },
          {
            pattern: /^[\u4e00-\u9fa5A-Za-z0-9 %$@._\-\/()\[\]{}（）【】｛｝]+$/,
            message: this.$t("datasource.fileurlTip")
          }
        ],
      },

      uploadUrl: process.env.VUE_APP_X_API + `/upload`,
      csvColumns: [],
      sample_values: [],
      localcsv: {},
      dbOptions: [],
      extractArr: [],
      loading: false,
    };
  },
  async mounted() {
    if (this.isEditable || this.isViewable) {
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
      this.fileForm.fileurl = this.fileList
        .map((item) => {
          return item.response[0];
        })
        .join("");
      let parseParam = this.getCsvParseParam()
      let result = await getCSVColumns(
        this.fileForm.fileurl,
        "csv",
        parseParam
      );
      this.csvColumns = result.file_header.column_names;
      if (result && !result.sample_values) {
        this.$error(this.$t('datasource.transformer.emptySampleValues'))
        return
      }
      this.sample_values = result.sample_values ?? [];
      this.formatCsvTransformerData(this.csvColumns, this.sample_values);
    }
  },
  methods: {
    submitUrl() {
      let flag = false;
      this.$refs.fileform.validate((valid) => {
        if (valid) {
          flag = true;
        } else {
          flag = false;
        }
      });
      return flag;
    },
    //获取transformer的参数
    // getTransformerParams(data) {
    //   this.transformerParser = data;
    // },
    handleRemove(file, filelist) {
      this.fileList = filelist;
    },
    //编辑状态的回显
    echoEditData() {
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
      this.fileList = [].concat(file);
      this.showfiletip = false;
      this.$store.commit("app/SET_CSV_FILES", this.fileList);

      this.getCsvColumnsData();
    },
    submitUpload() {
      let isbreak=true
      if (this.activeName == "first") {
        if (this.fileList.length == 0) {
          this.showfiletip = true;
          isbreak=false;
        }
      } else {
        isbreak=this.submitUrl();
      }

      if(!isbreak){
        return isbreak
      }
      if (!this.showTransformer) {
        Message.closeAll();
        Message({
          type: "warning",
          message: this.$t("datasource.transformer.nexttip"),
        });

        // Message.warning(this.$t("datasource.transformer.nexttip"));
        isbreak= false;
      } else {
        this.$nextTick(() => {
          this.$refs.transform.getTransformerParams();
          if (this.$refs.transform.isbreak) isbreak= false;
        });
      }
      return isbreak
    },

    async getCsvColumnsData() {
      try {
        this.loading = true;
        this.showfiletip = false;
        if (this.activeName == "first" && this.fileList.length == 0) {
          this.showfiletip = true;
          return;
        }
        this.submitUrl();
        if (this.activeName == "second" && !this.fileForm.fileurl) {
          return;
        }
        this.$refs.param.submit();
        if(!this.$refs.param.isValid)return 
        this.showTransformer = false;
        this.$store.commit("app/SET_CSV_TRANSFORMER_PARSER", null);

        if (this.isEditable) {
          this.$parent.$parent.isEditable = false;
        }
        this.csvColumns = [];
        this.dbOptions = [];
        let result = null;
        let parseParam = this.getCsvParseParam()

        if (this.activeName == "first") {
          if (this.$refs.param.isValid && this.fileList.length > 0) {
            result = await getCSVColumns(
              this.fileList.map((item) => {
                return item.response[0];
              }),
              "csv",
              parseParam
            );
          } else {
            return;
          }
        } else {
          result = await getCSVColumns(
            this.fileForm.fileurl,
            "csv",
            parseParam
          );
        }

        this.loading = false
        if (result && result.message) {
          this.$error(result.message);
          return;
        }
        
        const columns = result.file_header.column_names;
        const columnInObj = {};
        const columnRegexPattern = /^[a-zA-Z_][a-zA-Z0-9_]*$/;
        for (let i = 0; i < columns.length; i++) {
          if (columns[i] === "") {
            this.$error(this.$t('datasource.transformer.emptyColumnName') + columns.join(", "));
            return
          }
          if (!columnRegexPattern.test(columns[i])) {
            this.$error(this.$t('datasource.transformer.invalidColumnName') + columns[i]);
            return
          }
          if (columnInObj[columns[i]]) {
            this.$error(this.$t('datasource.transformer.duplicateColumnName') + columns[i]);
            return
          }
          columnInObj[columns[i]] = true;
        }

        if (result && !result.sample_values) {
          this.$error(this.$t('datasource.transformer.emptySampleValues'));
          return
        }

        this.csvColumns = result.file_header.column_names;
        this.sample_values = result.sample_values ?? [];


        
        // 去掉自定义列
        // if (this.$refs.param.ruleForm.customcol) {
        //   let apiColumns = result.file_header.column_names;
        //   let localcolumns = this.$refs.param.ruleForm.customcol.split(",");
        //   if (localcolumns.length != apiColumns.length) {
        //     this.$error(this.$t("datasource.transformer.csvtip"));
        //     return;
        //   }
        //   this.csvColumns = this.$refs.param.ruleForm.customcol.split(",");
        //   this.sample_values = result.sample_values.map((item) => {
        //     return item.slice(0, localcolumns.length);
        //   });
        // }
        this.formatCsvTransformerData(this.csvColumns, this.sample_values);
        this.showConfig = true;

        this.submitUpload();
        this.loading = false
      } catch (error) {
        this.loading = false
        error && error.message && this.$error(error.message);
      }
    },
    //组合CSV的transfomrer页面需要的数据
    formatCsvTransformerData(columns, values) {
      let inputList = values.map((item) => {
        return Object.fromEntries(
          item.map((val, index) => {
            return [this.csvColumns[index], val];
          })
        );
      });
      let msgBody = values.map((item) => {
        return item;
      });
      // 自定义列删除
      // if (this.$store.state.app.csvTransformerlocalCols.length > 0) {
      //   msgBody.unshift(
      //     this.$store.state.app.csvTransformerlocalCols.toString()
      //   );
      // } else {
      //   msgBody.unshift(columns.toString());
      // }
      msgBody.unshift(columns.toString());
      this.extractArr.splice(0, this.extractArr.length);
      columns.forEach((item) => {
        let obj = {};
        obj["columns"] = columns.map((val) => {
          return {
            description: item,
            name: item,
            show: true,
            type: "varchar",
            value: "",
          };
        });
        (obj["columnname"] = ""), (obj["expression"] = ""), (obj["type"] = "");
        this.extractArr.push(obj);
      });
      let csvTransformer = {
        columns:
          this.$store.state.app.csvTransformerlocalCols.length > 0
            ? this.$store.state.app.csvTransformerlocalCols
            : columns,
        inputList: this.$store.state.app.csvParser
          ? this.$store.state.app.csvParser.input
          : inputList,
        msgBody: msgBody.join("\n"),
      };
      let transformerColumns = [
        {
          value: "expression",
          label: this.$t("expression"),
          children: this.maptypes.map((item) => {
            return {
              value: item,
              label: item,
            };
          }),
        },
        {
          value: "mapping",
          label: this.$t("mapping"),
          children: csvTransformer["columns"].map((item) => {
            return {
              value: item,
              label: item,
            };
          }),
        },
      ];
      this.$store.commit("app/SET_TRANSFORMER_MAPCOLUMNS", transformerColumns);
      this.$store.commit("app/SET_CSV_TRANSFORMER_PARSER", csvTransformer);
      this.showTransformer = true;
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
        // error&&error.desc&&this.$error(error.desc)
      }
    },
    //获取 csv 解析需要的参数
    getCsvParseParam() {
      let dsn = ''
      dsn = getDsnData(this.sourceParent.sourceForm.data, this.sourceParent.currentDefinition)
      dsn = dsn?.split('?')[1]?.split('&read_concurrency')[0] ?? ''
      return dsn;
    },
    checkFileName(file) {
      const regex = /^[\u4e00-\u9fa5A-Za-z0-9 %$@._\-()\[\]{}（）【】｛｝]+$/;
      const fileName = file.name;
      if (!regex.test(fileName)) {
        this.$error(this.$t('datasource.supportCharacter'));
        return false; // 不允许上传
      }

      return true; // 允许上传
    }
  },
  watch:{
    "$i18n.locale": {
      deep: true,
      handler(val) {
        this.$nextTick(()=>{
          this.showfiletip=false
        })
        
      },
    },
    "fileForm.fileurl": {
      handler(val) {
        this.$store.commit("app/SET_CSV_FILES", val);
      }
    }
  }
};
</script>
<style lang="scss" scoped>
:deep(.markdown-body) {
  p {
    font-size: 14px;
  }
  color: $color-description;
}
.upload-demo {
  display: flex;
  align-items: baseline;
}
.csv-data {
  // width: 600px;
  // padding: 5px;
  // box-sizing: border-box;
  // border: 1px solid #e3e4e6;
  // margin-bottom: 20px;
  // border-radius: 12px;
  // padding: 15px;
  .upload-file {
    display: flex;
    margin-bottom: 18px;
    align-items: baseline;
    .label {
      padding-right: 40px;
      color: #4259ce;
      width: 220px;
      font-weight: 500;
      font-size: 14px;
      text-align: left;
      position: relative;

      &.required {
        padding-left: 10px;
        &::before {
          content: "*";
          color: red;
          font-size: 16px;
          line-height: 25px;
          left: 0px;
          position: absolute;
        }
        &.en {
          width: 225px;
        }
      }
    }
    .el-input {
      flex: 1;
    }
  }
  .nextbtn {
    width: 100%;
  }
  .csv-config {
    margin-bottom: 20px;
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
