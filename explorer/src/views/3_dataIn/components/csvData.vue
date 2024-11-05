<template>
  <div class="csv-data">
    <div class="file-settings">
      <el-tabs v-model="fileForm.file_or_dir">
        <el-tab-pane :label="$t('datasource.uploadcsv')" name="1" :disabled="isModifying && fileForm.file_or_dir === '2'">
          <div class="upload-file">
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
                multiple
                :on-remove="handleRemove"
                :data="uploadData"
                :action="uploadUrl"
                :on-success="handleSuccess"
                :before-upload="checkFileName"
                :file-list="fileList"
                :auto-upload="true"
                :disabled="isModifying"
                size="small"
              >
                <el-button slot="trigger" size="small" type="primary" plain :disabled="$COMMUNITY || isModifying">{{
                  $t("datasource.selectfile")
                }}</el-button>
              </el-upload>
            </el-tooltip>
            <span
              style="color: red; font-size: 12px;"
              v-if="showfiletip"
              >{{ this.$t("datasource.uploadcsvtip") }}</span
            >
          </div>
          <div style="margin-bottom: 20px;">
            <el-form
            :model="fileForm"
            ref="fileform"
            :rules="fileRules"
            label-width="220px"
          >
            <el-switch
              v-model="fileForm.keep_processed_files"
              :active-text="$t('datasource.keepProcessedFile')">
            </el-switch>
            </el-form>
          </div>
        </el-tab-pane>
        <el-tab-pane :label="$t('datasource.configcsv')" name="2" :disabled="isModifying && fileForm.file_or_dir === '1'">
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
                      :content="$t('datasource.csvFileDesc')"
                    />
                  </template>
                  <span>
                    <span>{{ $t('datasource.csvFileDir') }}</span>
                    <span style="margin-left: 4px">
                      <i class="el-icon-info"></i>
                    </span>
                  </span>
                </el-tooltip>
              </template>
              <el-input size="small" id="fileurl" v-model="fileForm.fileurl" :disabled="isModifying"></el-input>
            </el-form-item>
            <el-form-item prop="filepattern">
              <template slot="label">
                <el-tooltip placement="top" effect="light" :open-delay="0">
                  <template slot="content">
                    <DocsContent
                      :content="$t('datasource.csvFilePatternDesc')"
                    />
                  </template>
                  <span>
                    <span>{{ $t('datasource.csvFilePattern') }}</span>
                    <span style="margin-left: 4px">
                      <i class="el-icon-info"></i>
                    </span>
                  </span>
                </el-tooltip>
              </template>
              <el-input size="small" id="filepattern" v-model="fileForm.file_pattern" :disabled="isModifying"></el-input>
            </el-form-item>
            <el-form-item prop="filenotify">
              <template slot="label">
                <el-tooltip placement="top" effect="light" :open-delay="0">
                  <template slot="content">
                    <DocsContent
                      :content="$t('datasource.csvNewFileNotifyDesc')"
                    />
                  </template>
                  <span>
                    <span>{{ $t('datasource.csvNewFileNotify') }}</span>
                    <span style="margin-left: 4px">
                      <i class="el-icon-info"></i>
                    </span>
                  </span>
                </el-tooltip>
              </template>
              <el-switch v-model="fileForm.new_file_notify">
              </el-switch>
            </el-form-item>
            <el-form-item prop="notifyinterval" v-if="fileForm.new_file_notify">
              <template slot="label">
                <el-tooltip placement="top" effect="light" :open-delay="0">
                  <template slot="content">
                    <DocsContent
                      :content="$t('datasource.csvNotifyIntervalDesc')"
                    />
                  </template>
                  <span>
                    <span>{{ $t('datasource.csvNotifyInterval') }}</span>
                    <span style="margin-left: 4px">
                      <i class="el-icon-info"></i>
                    </span>
                  </span>
                </el-tooltip>
              </template>
              
              <el-input-number id="notifyinterval" size="small" v-model="fileForm.notify_interval" :min="1" :max="600">
                
              </el-input-number>
              <span style="margin-left: 10px;">{{ $t('seconds') }}</span>
            </el-form-item>
            <el-form-item prop="filesort" v-if="fileForm.new_file_notify">
              <template slot="label">
                <el-tooltip placement="top" effect="light" :open-delay="0">
                  <template slot="content">
                    <DocsContent
                      :content="$t('datasource.csvFileSortDesc')"
                    />
                  </template>
                  <span>
                    <span>{{ $t('datasource.csvFileSort') }}</span>
                    <span style="margin-left: 4px">
                      <i class="el-icon-info"></i>
                    </span>
                  </span>
                </el-tooltip>
              </template>
              <el-radio v-model="fileForm.sort" label="1">{{ $t('sortasc') }}</el-radio>
              <el-radio v-model="fileForm.sort" label="2">{{ $t('sortdesc') }}</el-radio>
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
import { getCSVOptions } from "../utils";
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
  data() {
    return {
      isModifying: false,
      showfiletip: false,
      maptypes: ["value", "generator", "join", "format", "sum", "expr"],
      showTransformer: false,
      transformerParser: null,
      language: localStorage.getItem("local_language"),
      uploadData: {
        req_id: new Date().getTime(),
      },
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
      fileRules: {
        fileurl: [
          {
            required: true,
            // trigger: "blur",
            message: this.$t("datasource.inputcsvdir"),
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
    if (!this.isEditable && !this.isViewable) {
      return;
    }

    this.isModifying = this.$store.state.app.currentEditID > 0;

    let csvFileConfig = this.$store.state.app.csvFileListener;
    for (let configItem in csvFileConfig) {
      if (configItem == "keep_processed_files" || configItem == "new_file_notify") {
        this.fileForm[configItem] = csvFileConfig[configItem] == "true";
      } else if (configItem == "notify_interval" && csvFileConfig[configItem] && csvFileConfig[configItem].endsWith("s")) {
        this.fileForm[configItem] = csvFileConfig[configItem].slice(0, -1);
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
    
    let parseParam = this.getCsvParseParam()
    let result = await getCSVColumns(fileUrl,"csv",parseParam);
    this.csvColumns = result.file_header.column_names;
    if (result && !result.sample_values) {
      this.$error(this.$t('datasource.transformer.emptySampleValues'))
      return
    }
    this.sample_values = result.sample_values ?? [];
    this.formatCsvTransformerData(this.csvColumns, this.sample_values);
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

    handleSuccess(response, file, fileList) {
      if (fileList.length > 2) {
        fileList.splice(fileList.length - 1, 1);
      }

      this.fileList = fileList;
      this.showfiletip = false;
      this.$store.commit("app/SET_CSV_FILES", this.fileList);
    },
    csvFileInputOK() {
      if (this.fileForm.file_or_dir == "1" && this.fileList.length == 0) {
        this.showfiletip = true;
        Message.warning(this.$t('datasource.uploadcsvtip'));
        return false;
      } else if (this.fileForm.file_or_dir == "2" && !this.fileForm.fileurl) {
        Message.warning(this.$t('datasource.inputcsvdir'))
        this.submitUrl();
        return false;
      }
      return true;
    },

    submitUpload() {
      let isbreak = this.csvFileInputOK();
      if (!isbreak) {
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
        if (!this.csvFileInputOK()) {
          this.loading = false;
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
  
        let parseParam = this.getCsvParseParam()
        let fileUrl = this.fileForm.file_or_dir == "1" 
                    ? this.fileList.map(item => item.response[0]).join(",")
                    : this.fileForm.fileurl;

        let result = await getCSVColumns(fileUrl, "csv", parseParam);
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

        this.formatCsvTransformerData(this.csvColumns, this.sample_values);
        this.submitUpload();
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
      let options = getCSVOptions(this.sourceParent.sourceForm.data, this.sourceParent.currentDefinition)
      return options.join("&");
    },
    checkFileName(file) {
      const regex = /^[\u4e00-\u9fa5A-Za-z0-9 %$@._\-()\[\]{}（）【】｛｝]+$/;
      const fileName = file.name;
      if (!regex.test(fileName)) {
        this.$error(this.$t('datasource.supportCharacter'));
        return false; // 不允许上传
      }

      for (let i = 0; i < this.fileList.length; i++) {
        if (this.fileList[i].name === fileName) {
          if (!confirm("有重名文件，是否要覆盖文件？")) {
            return false;
          }
        }
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
    "fileForm": {
      deep: true,
      handler(val) {
        this.$store.commit("app/SET_CSV_FILE_LISTENER", val);
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
//  display: flex;
//  align-items: baseline;
   width: 300px;
}
.csv-data {
  // width: 600px;
  // padding: 5px;
  // box-sizing: border-box;
  // border: 1px solid #e3e4e6;
  // margin-bottom: 20px;
  // border-radius: 12px;
  // padding: 15px;
  .el-upload {
    text-align: left;
  }

  .upload-file {
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
