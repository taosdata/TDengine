<template>
  <div class="csv-data">
    <div v-if="fileForm.file_or_dir == '1'" class="file-settings" >
      <BlockHeader :title="$t('datasource.uploadcsv')" />
      <div style="padding-top: 10px;">
        <div v-for="csvfile in fileList" :key="csvfile.name">{{ csvfile.response[0] }}</div>
      </div>
      <div>
        <span v-if="fileForm.keep_processed_files">{{ $t('datasource.keepProcessedFile') }}</span>
        <span v-else>{{ $t('datasource.deleteProcessedFile') }}</span>
      </div>
    </div>
    <div v-else>
      <BlockHeader :title="$t('datasource.configcsv')" />
      <div class="descriptions" style="padding-top: 10px;">
        <div class="descItem">
          <span class="itemTitle">{{ $t('datasource.csvFileDir') }}:</span>
          <span>{{ fileForm.fileurl }}</span>
        </div>
        <div class="descItem">
          <span class="itemTitle">{{ $t('datasource.csvFilePattern') }}:</span>
          <span>{{ fileForm.file_pattern }}</span>
        </div>
        <div class="descItem">
          <span class="itemTitle">{{ $t('datasource.csvNewFileNotify') }}:</span>
          <span>{{ fileForm.new_file_notify ? $t('yes') : $t('no') }}</span>
        </div>
        <div class="descItem" v-if="fileForm.new_file_notify">
          <span class="itemTitle">{{ $t('datasource.csvNotifyInterval') }}:</span>
          <span>{{ fileForm.notify_interval }}{{ $t('seconds') }}</span>
        </div>
        <div class="descItem" v-if="fileForm.new_file_notify">
          <span class="itemTitle">{{ $t('datasource.csvFileSort') }}:</span>
          <span v-if="fileForm.sort === '1'">{{ $t('sortasc') }}</span>
          <span v-else>{{ $t('sortdesc') }}</span>
        </div>
      </div>
    </div>
    <CsvParameter ref="param" :echoData="echoData" :isEditable="isEditable">
      <template v-slot:next>
        <CommonTransformer
          ref="transform"
          :parserColumns="extractArr"
          v-if="showTransformer"
        ></CommonTransformer>
      </template>
    </CsvParameter>
  </div>
</template>
<script>
import CsvParameter from "./csv/csvParameter.vue";
import CsvColumn from "./csv/csvColumn.vue";
import { deepClone } from "@/utils";
import { getDsnData, handleDownload } from "../utils";
import { sendSQLReq } from "@/api/gateway/console";
import { getCSVColumns } from "@/api/explorer/datain";
import { Message } from "element-ui";
import CommonTransformer from "./transformerInfo.vue";
import DocsContent from "@/views/support/components/editorContentDisplay.vue";
import BlockHeader from "./blockHeader.vue";
export default {
  name: "CsvData",
  components: { CsvParameter, CsvColumn, CommonTransformer, DocsContent, BlockHeader },
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
 
      fileList: [],
      fileForm: {
        file_or_dir: "1",
        fileurl: "",
        file_pattern: "",
        new_file_notify: false,
        notify_interval: "30",
        sort: "1",
        keep_processed_files: false,
      },

      csvColumns: [],
      sample_values: [],
      localcsv: {},
      dbOptions: [],
      extractArr: [],
      loading: false,
    };
  },
  async mounted() {
    
    let csvFileConfig = this.$store.state.app.csvFileListener;
    for (let configItem in csvFileConfig) {
      if (configItem == "keep_processed_files" || configItem == "new_file_notify") {
        this.fileForm[configItem] = csvFileConfig[configItem] == "true";
      } else if (configItem == "notify_interval") {
        let value = csvFileConfig[configItem];
        if (typeof value == "string" && csvFileConfig[configItem].endsWith("s")) {
          this.fileForm[configItem] = csvFileConfig[configItem].slice(0, -1);
        } else {
          this.fileForm[configItem] = value;
        }
      } else {
        this.fileForm[configItem] = csvFileConfig[configItem];
      }
    }

    //编辑状态直接从返回值去csv 的parser
    let fileUrl = csvFileConfig.fileurl;
    if (csvFileConfig.file_or_dir == "1") {
      fileUrl = this.$store.state.app.csvfiles;
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
      this.$store.commit("app/SET_CSV_FILES", this.fileList);
    }

      //编辑状态直接从返回值去csv 的parser
      this.showConfig = true;
      
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
    
  },
  methods: {
    handleDownloadFile(val) {
      if (val) {
        let name = val?.substr(val.lastIndexOf("/") + 1)
        handleDownload(val, name)
      }
    },
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
  .mb5 {
    margin-bottom: 5px;
  }
  .descriptions {
    font-size: 16px;
    display: grid;
    grid-template-columns: 1fr 1fr;
    .descItem {
      padding: 0 5px 10px 0;
      .itemTitle {
        padding-right: 10px;
      }
    }
  }
    

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
