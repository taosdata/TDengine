<template>
  <div :class="['extract-split',itemData.columnname&&itemData.columnname==$store.state.app.transresultname?'active':'']">
    <div class="extract-item">
      <el-form :model="ruleForm" :rules="rules" size="small" ref="extractForm">
        <el-form-item prop="col_name">
          <el-select
            size="small"
            :placeholder="$t('datasource.transformer.col_select')"
            v-model="ruleForm.col_name"
            @change="selectCol"
            :disabled="ruleForm.col_name != '' && itemData.columnname != ''"
          >
            <el-option
              v-for="(item, index) in extractColumns"
              :key="index"
              :label="item.name"
              :value="item.name"
              :disabled="!item.show"
            ></el-option>
          </el-select>
        </el-form-item>
        <el-form-item prop="filter_name">
          <el-select
            size="small"
            :placeholder="$t('datasource.transformer.filter_type')"
            v-model="ruleForm.filter_name"
            @change="changeExtractType"
            :disabled="isViewable"
          >
            <el-option
              v-for="item in extractTypes"
              :key="item"
              :label="item"
              :value="item"
              :disabled="item == 'join' && itemData.value_type !== 'array'"
            ></el-option>
          </el-select>
        </el-form-item>
        <el-form-item prop="filter_expres">
          <template v-if="ruleForm.filter_name == 'split'">
            <SplitExpression
              ref="splitExpression"
              :ruleForm="itemData.splitParams"
            ></SplitExpression>
          </template>
          <el-input v-else
            size="small"
            :placeholder="$t('datasource.transformer.expre_' + ruleForm.filter_name)"
            v-model="ruleForm.filter_expres"
            @input="changeExtractExpr"
            :disabled="isViewable"
          ></el-input>
        </el-form-item>
      </el-form>

      <div class="btns" style="display: flex" v-if="!isViewable">
        <el-button
          icon="el-icon-delete"
          @click="deleteExtract"
          style="display: flex"
        ></el-button>
        <el-button
          icon="el-icon-PREVIEW"
          @click="submit"
          style="display: flex"
        ></el-button>
      </div>
    </div>
    <ul class="col-list" v-if="tableColumns.length > 0 && !isViewable">
      <li v-for="(item, index) in tableColumns.slice(0, 9)" :key="index">
        <span>{{ item.name }}</span>
      </li>
      <li v-if="tableColumns.length > 9" >
        <el-tooltip
          :content="$t('datasource.transformer.viewmore')"
          placement="top"
          effect="light"
        >
          <span @click='submit'><i class="el-icon-more"></i></span>
        </el-tooltip>
      </li>
    </ul>
  </div>
</template>
<script>
import { getParser, checkParseData } from "@/api/explorer/datain";
import { parsinginZone } from "@/utils";
import SplitExpression from "./splitExpression.vue";
import { deepClone } from "@/utils";
export default {
  name: "ExtractSplit",
  components: { SplitExpression },
  inject: ['sourceParent'],
  props: {
    itemData: {
      type: Object,
      default: () => {
        return null;
      },
    },
    index: {
      type: Number,
      default: 0,
    },
    extractColumns: {
      type: Array,
      default: () => {
        return [];
      },
    },
    indentifiedColumns: {
      type: Array,
      default: () => {
        return [];
      },
    },
  },
  data() {
    return {
      splitParams: {
        sep: "",
        n: "",
        names: "",
      },
      joinParams: {
        join_with: "",
      },
      isJson: true,
      mqttDefaultCols: ["topic", "qos", "payload"],
      kafkaDefaultCols: ["topic", "partition", "offset", "key", "value"],
      mongodbDefaultCols: ["value"],
      disabled: false,
      splitExpre: {},
      extractParseData: {},
      maptypes: ["value", "generator", "join", "format", "sum", "expr"],
      tableColumns: [],
      extractTypes: ["split", "regex", "join"],
      ruleForm: {
        col_name: "",
        filter_name: "",
        filter_expres: "",
      },
      rules: {
        col_name: [
          {
            required: true,
            trigger: "change",
            message: this.$t("datasource.transformer.col_select"),
          },
        ],
        filter_name: [
          {
            required: true,
            trigger: "change",
            message: this.$t("datasource.transformer.filter_type"),
          },
        ],
        filter_expres: [
          {
            required: false,
            trigger: "blur",
            message: this.$t("datasource.transformer.expre_input"),
          },
        ],
      },
      showTable: false,
      tableData: [],
    };
  },
  methods: {
    //提交验证
    validateExtreact() {
      let isbreak=false
      if (this.ruleForm.filter_name == "split") {
        this.$refs.splitExpression.submit();
        if (!this.$refs.splitExpression.isValid) {
          isbreak=true;
        }
      }else{
        if(this.ruleForm.filter_expres){
          this.$refs.extractForm.validate(valid=>{
            if(valid){
              return true
            }else{
              return false
            }
          })
          isbreak=true
        }
      }
      return isbreak
    },
    async showResultTable() {
      await this.submitExtract(true);
      this.$store.commit("app/SET_TRANS_RESULT_NAME", this.itemData.columnname);
    },
    changeExtractExpr(val) {
      this.$emit("changeExtractExpr", this.ruleForm.col_name, val);
    },
    initData(val) {
      this.ruleForm.col_name = val.columnname;
      this.ruleForm.filter_expres = val.expression;
      this.ruleForm.filter_name = val.type;
    },
    selectCol() {
      this.disabled = true;
      this.$emit("selectColumn", this.index, this.ruleForm.col_name);
    },
    changeExtractType() {
      let index = this.$parent.extractArr.findIndex(
        (item) => item.columnname == this.ruleForm.col_name
      );
      this.$set(
        this.$parent.extractArr[index],
        "type",
        this.ruleForm.filter_name
      );
    },
    submit() {
      this.$parent.validateMsgBody();
      if (!this.$parent.msgForm.msgbody) {
        return;
      }
      if (this.ruleForm.filter_name == "split") {
        this.$refs.splitExpression.submit();
        if (!this.$refs.splitExpression.isValid) {
          return;
        }
      }
      this.$refs.extractForm.validate(async (valid) => {
        if (valid) {
          this.$store.commit(
            "app/SET_TRANS_RESULT_NAME",
            this.itemData.columnname
          );
                      await this.submitExtract();
                    await this.submitExtract(true);

          return true;
        } else {
          return false;
        }
      });
    },
    async getParserData(data, isall) {
      try {
        let checkResult = checkParseData(data);
        if (checkResult) {
          this.$message.warning(this.$t(checkResult));
          return;
        }
        let result = await getParser(data);
        if (result.message) {
          this.$error(result.message);
          return;
        }

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
            children: result[0].fields.map((item) => {
              return {
                value: item.name,
                label: item.name,
              };
            }),
          },
        ];

        let colLists = [];
        let tbdata = [];

        colLists =(
          this.$store.state.app.currentDBType == "csv"
            ? result[0].fields
            : result[0].fields
                .filter((val) => {
                  if (
                    this.$store.state.app.currentDBType == "mqtt" &&
                    !this.mqttDefaultCols.includes(val.name)
                  ) {
                    return val;
                  }
                  if (
                    this.$store.state.app.currentDBType == "kafka" &&
                    !this.kafkaDefaultCols.includes(val.name)
                  ) {
                    return val;
                  } else if(this.$store.state.app.supportSQL){
                    return val
                  }
                  if (
                    this.$store.state.app.currentDBType == "mongodb" &&
                    !this.mongodbDefaultCols.includes(val.name)
                  ) {
                    return val;
                  } else if(this.$store.state.app.supportSQL){
                    return val
                  }
                })
        ).map((item) => {
          return {
            description: item.name,
            name: item.name,
            show: true,
            type: "string",
            localType: item.type,
          }
        })       

        tbdata = result[0].columns.map((data) => {
          return Object.fromEntries(
            result[0].fields.map((item, index) => {
              return [item.name, 
                this.filterEmpty(data[index]) 
                ? (Array.isArray(data[index]) ? JSON.stringify(data[index]) : data[index].toString()) 
                : null];
            })
          );
        });

        if (isall) {
          transformerColumns.splice(1, 1, {
            value: "mapping",
            label: this.$t("mapping"),
            children: result[0].fields.map((item) => {
              return {
                value: item.name,
                label: item.name,
              };
            }),
          });
          this.$store.commit(
            "app/SET_TRANSFORMER_MAPCOLUMNS",
            transformerColumns
          );
          // 将当前提取或拆分的数据放在 table 的最前面 
          let resultData = []
          tbdata.map((item,index) => {
            this.tableData.map(addItem => {
              Object.keys(addItem).forEach(key => {
                delete item[key];
              });
            });
            let addObj = this.tableData[index];
            let newItem = {...addObj, ...item};
            resultData.push(newItem)
          });
          if (!this.isViewable) {
            this.$store.commit('app/SET_RESULTTB_SHOW',true)
          }
          this.$store.commit("app/SET_RESULTTB_TITLE_SHOW", 'extractResTb');
          this.$store.commit("app/SET_TRANS_RESULT_TABLE", resultData);
          this.$store.commit("app/SET_STB_DEFAULT_COLUMNS",colLists);
          
          return;
        }
        // 增量的 MAPCOLUMNS 不做存储
        // this.$store.commit(
        //   "app/SET_TRANSFORMER_MAPCOLUMNS",
        //   transformerColumns
        // );
        this.tableColumns = colLists.map((item) => {
          let obj = {};
          let finalVal = tbdata.map(
            (val) =>
              val[
                this.$store.state.app.currentDBType == "csv" ? item.name : item.name
              ]
          );
          obj.name =
            this.$store.state.app.currentDBType == "csv" ? item.name : item.name;
          obj.value = finalVal.join("") ? finalVal.join(" ; ") : "";
          return obj;
        });
        // this.singleFileds = result[0].fields
        this.tableData = tbdata;
        this.$store.commit("app/SET_ACTIVE_COLS", Object.keys(tbdata[0]));
      } catch (error) {
        console.log(error);
      }
    },
    //编辑回显调用接口
    echoExtract() {
      if (this.$store.state.app.transformExtractParseData) {
        this.getParserData(this.$store.state.app.transformExtractParseData);
      }
    },
    //提交单个
    async submitExtract(isall) {
      let inputList = [];
      let resultMsgbody = "";
      if (
        this.$parent.msgForm.msgbody.replace(/\}\s*\{/g, "}{").includes("}{")
      ) {
        //多json对象
        resultMsgbody = this.$parent.msgForm.msgbody
          .replace(/\}\s*\{/g, "}&${")
          .split("&$");
        this.isJson = true;
      } else {
        if (
          /\n/g.test(this.$parent.msgForm.msgbody) &&
          /^[^\{]/.test(this.$parent.msgForm.msgbody.trim())
        ) {
          //普通文本，目前第一列暂时不能为json格式
          resultMsgbody = this.$parent.msgForm.msgbody
            .replace(/[\n\s]/g, "*&$*")
            .split("*&$*");
          this.isJson = false;
        } else {
          try {
            if (
              /^\{/g.test(this.$parent.msgForm.msgbody) &&
              JSON.parse(this.$parent.msgForm.msgbody)
            ) {
              //单json对象
              resultMsgbody = [].concat(this.$parent.msgForm.msgbody);
              this.isJson = true;
            }
          } catch (error) {
            this.$error(this.$t("datasource.transformer.jsontip"));
            return;
          }

          resultMsgbody = this.$parent.msgForm.msgbody.split(";");
        }
      }
      let hiddenCols = [];
      if (!isall) {
        if (this.$store.state.app.currentDBType == "mqtt") {
          hiddenCols = ["ts", "qos", "topic"];
        }
        if (this.$store.state.app.currentDBType == "kafka") {
          hiddenCols = ["ts", "topic", "partition", "offset", "key"];
        }
        if (this.$store.state.app.currentDBType == "mongodb") {
          hiddenCols = ["ts"];
        }
      } else {
        hiddenCols = [];
      }

      inputList = resultMsgbody.map((msg) => {
        let inputobj = {};
        this.indentifiedColumns
          .filter((val) => !hiddenCols.includes(val.name))
          .forEach((item) => {
                        if (this.$store.state.app.currentDBType == "mqtt") {
              if (item.name == "payload") {
                                inputobj["payload"] = isall
                  ? msg
                  : this.isJson
                    ? JSON.stringify({
                        [`${this.itemData.columnname}`]:
                          JSON.parse(msg)[this.itemData.columnname],
                      })
                    : msg;
                                } else {
                inputobj[item.name] =
                  item.type == "timestamp"
                    ? parsinginZone(new Date())
                    : item.name;
              }
            } else if (this.$store.state.app.currentDBType == "kafka") {
              if (item.name == "value") {
                inputobj["value"] = isall
                  ? msg
                  : this.isJson
                  ? JSON.stringify({
                      [`${this.itemData.columnname}`]:
                        JSON.parse(msg)[this.itemData.columnname],
                    })
                  : msg;
              } else {
                inputobj[item.name] =
                  item.type == "timestamp"
                    ? parsinginZone(new Date())
                    : item.name;
              }
            } else if (this.$store.state.app.currentDBType == "mongodb") {
              if (item.name == "value") {
                inputobj["value"] = isall
                  ? msg
                  : this.isJson
                  ? JSON.stringify({
                      [`${this.itemData.columnname}`]:
                        JSON.parse(msg)[this.itemData.columnname],
                    })
                  : msg;
              } else {
                inputobj[item.name] =
                  item.type == "timestamp"
                    ? parsinginZone(new Date())
                    : item.name;
              }
            }
          });
        return inputobj;
      });
      this.extractParseData = {
        extract: {},
      };
      let currentextractArr=deepClone(this.$parent.extractArr)
      if(this.tableColumns.length>0){
        //有列说明已经请求过需要区分split和regex，作为api参数
      }else{
        // api参数需要过滤掉
      }
      deepClone(this.$parent.extractArr)
        .map((item) => {
          let splitobj = null;
          if (item.type == "split") {
            splitobj = Object.fromEntries(
              Object.entries(item?.splitParams).filter(([key, value]) => {
                return value !== null && value != undefined && value != "";
              })
            );
            splitobj["n"] = Number(splitobj["n"]);
            Object.hasOwnProperty.call(splitobj, "names")
              ? (splitobj["names"] = splitobj["names"].split(","))
              : splitobj;
          }
          return {
            [`${item.columnname}`]: {
              [`${item.type}`]:
                item.type == "regex" || item.type == "join"
                  ? item.expression
                  : item.type == "split"
                  ? splitobj
                  : item.expression
                  ? item.expression.split(";").map((item) => item.trim())
                  : item.expression,
            },
          };
        })
        .forEach((val) => {
          Object.assign(this.extractParseData["extract"], val);
        });
      let topparse = deepClone(this.$store.state.app.topParse);
      
      const keys = Object.keys(this.extractParseData.extract);
      const slicedKeys = keys.slice(0, this.index + 1);
      const slicedObj = slicedKeys.reduce((acc, key) => {
        acc[key] = this.extractParseData.extract[key];
        return acc;
      }, {});

      topparse["parser"]["mutate"] = isall
        ? [].concat({ extract: slicedObj })
        : [].concat({
            extract: {
              [`${this.itemData.columnname}`]:
                this.extractParseData["extract"][this.itemData.columnname],
            },
          });
     
      let parser = {
        parser: {
          parse: this.$store.state.app.topParse.parser.parse,
          mutate: topparse["parser"]["mutate"],
        },

        input: this.$parent.isCSV
          ? isall
            ? this.$store.state.app.csvTransformerParser?.inputList
            : this.$store.state.app.csvTransformerParser?.inputList.map(
                (item) => {
                  if (Object.keys(item).includes(this.itemData.columnname)) {
                    return {
                      [this.itemData.columnname]:
                        item[this.itemData.columnname],
                    };
                  }
                }
              )
          :this.$store.state.app.supportSQL?isall?this.$store.state.app.topParse.input:[].concat(
            this.$store.state.app.topParse.input.map((item,index) => {
              return {
                [`${this.itemData.columnname}`]: 
                  this.$store.state.app.topParse.input[index][this.itemData.columnname]
              }
          })): inputList,
      };

      this.$store.commit("app/SET_EXTRACT_PARSE_DATA", this.extractParseData);
      if(!isall){
        switch(this.$store.state.app.currentDBType){
          case 'mqtt':
            if(Object.hasOwnProperty.call(parser.parser.parse.payload,'json')){
              parser.parser.parse.payload.json=''
            }
            break
          case 'kafka':
          if(Object.hasOwnProperty.call(parser.parser.parse.value,'json')){
              parser.parser.parse.value.json=''
            }
            break
          case 'mongodb':
          if(Object.hasOwnProperty.call(parser.parser.parse.value,'json')){
              parser.parser.parse.value.json=''
            }
            break
        }
      }
      await this.getParserData(parser, isall);
    },
    deleteExtract() {
      this.$emit("deleteExtract", this.index, this.ruleForm.col_name);
    },
    filterEmpty(val) {
      if (
        Object.is(val, undefined) | Object.is(val, "") ||
        Object.is(val, null)
        ) {
        return "";
      }
      if (Object.is(val, 0) || Object.is(val, false) || Object.is(val, true) || typeof val == 'object') {
        return val.toString();
      }
      return val;
    },
  },
  mounted() {
    if (this.itemData) {
      this.initData(this.itemData);
      if (this.itemData.columnname && this.itemData == this.ruleForm.col_name) {
        this.disabled = true;
      }
    }
  },
  watch: {
    itemData: {
      deep: true,
      handler(val) {
        this.initData(val);
      },
    },
  },
  computed: {
    isViewable() {
      return this.sourceParent.isViewable;
    },
  }
};
</script>
<style lang="scss" scoped>
@keyframes heart{
  0% {
    box-shadow: 0 0 5px #4259ce;
  };
  // 50%{
  //   box-shadow: 0 0 20px #4259ce;
  // }
  100%{
    box-shadow: 0 0 5px #4259ce;
  }
}
.extract-split {
  // &.active{
  //   padding: 20px;
  //   border-radius:6px;
  //   animation:heart 5s linear infinite;
  // }
  margin-bottom: 12px;
  .extract-item {
    display: flex;
    flex-wrap: nowrap;
    .el-form {
      display: grid;
      column-gap: 15px;
      grid-template-columns: 1.5fr 1.5fr 3fr;
    }
    .el-input:first-child {
      margin-left: 0px;
    }
    .btns {
      display: flex;
      flex-wrap: nowrap;
      .el-button {
        display: flex;
        align-items: center;
        justify-content: center;
        height: 32px;
        width: 32px;
        border-radius: 6px;
        /* border: 1px solid #4259ce; */
        &:first-child {
          margin-right: 10px;
          margin-left: 20px;
        }
      }
    }
  }
}
.table {
  max-height: 300px;
  overflow-y: auto;
}
.el-form-item--small.el-form-item {
  margin-bottom: 10px;
}
.col-list {
  margin-bottom: 25px;
  display: grid;
  grid-template-columns: 1fr 1fr 1fr 1fr 1fr;
  column-gap: 15px;
  row-gap: 15px;
  max-height: 80px;
  overflow-y: hidden;
  li {
    color: #4259ce;
    background: #ecf2fe;
    border-radius: 14px;
    border: 1px solid #f6f8fa;
    text-align: center;
  }
}
</style>
