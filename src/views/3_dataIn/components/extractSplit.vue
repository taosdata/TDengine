<template>
  <div class="extract-split">
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
          >
            <el-option
              v-for="item in extractTypes"
              :key="item"
              :label="item"
              :value="item"
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
          <el-popover
            v-else
            trigger="click"
            placement="top-start"
            :content="$t('datasource.transformer.mutiple')"
          >
            <el-input
              size="small"
              slot="reference"
              :placeholder="$t('datasource.transformer.expre_input')"
              v-model="ruleForm.filter_expres"
              @input="changeExtractExpr"
            ></el-input>
          </el-popover>
        </el-form-item>
      </el-form>

      <div class="btns" style="display: flex">
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
    <ul class="col-list" v-if="tableColumns.length > 0">
      <li v-for="(item, index) in tableColumns.slice(0, 9)" :key="index">
        <template v-if="item.value">
          <el-tooltip
            class="item"
            effect="light"
            :content="$t('datasource.transformer.sampleval') + ':' + item.value"
            placement="top-start"
          >
            <span>{{ item.name }}</span>
          </el-tooltip>
        </template>
        <span v-else>{{ item.name }}</span>
      </li>
      <li v-if="tableColumns.length > 9" @click="showResultTable">
        <el-tooltip
          :content="$t('datasource.transformer.viewmore')"
          placement="top"
          effect="light"
        >
          <span><i class="el-icon-more"></i></span>
        </el-tooltip>
      </li>
    </ul>
  </div>
</template>
<script>
import { getParser } from "@/api/explorer/datain";
import { Message } from "element-ui";
import { parsinginZone } from "@/utils";
import SplitExpression from "./splitExpression.vue";
import { deepClone } from "@/utils";
export default {
  name: "ExtractSplit",
  components: { SplitExpression },
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
      isJson: true,
      mqttDefaultCols: ["topic", "qos", "payload"],
      kafkaDefaultCols: ["topic", "partition", "offset", "key", "value"],
      disabled: false,
      splitExpre: {},
      extractParseData: {},
      maptypes: ["value", "generator", "join", "format", "sum", "expr"],
      tableColumns: [],
      extractTypes: ["split", "regex"],
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
    validateExtreact() {},
    async showResultTable() {
      // await this.submitExtract();
      await this.submitExtract(true);
      this.$nextTick(() => {
        if (document.querySelector(".transdescription")) {
          let dom = document.querySelector(".transdescription");
          let top = 2200//dom.offsetTop + document.body.scrollHeight;
          this.$store.commit("app/SET_TRANS_TABLE_HEIGHT", top);
        }
      });
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
      // this.$emit("setExtractName", this.index, this.ruleForm.col_name);
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
          // this.$store.commit("app/SET_TRANS_RESULT_NAME", "");
          await this.submitExtract();
          await this.submitExtract(true);
          this.$nextTick(() => {
            if (document.querySelector(".transdescription")) {
              let dom = document.querySelector(".transdescription");
              let top = 2200//dom.offsetTop + document.body.scrollHeight;
              this.$store.commit("app/SET_TRANS_TABLE_HEIGHT", top);
            }
          });
          this.$store.commit(
            "app/SET_TRANS_RESULT_NAME",
            this.itemData.columnname
          );
          Message.success(this.$t("datasource.successtip"));
          //执行完之后选中的列才能不会再被选中

          return true;
        } else {
          return false;
        }
      });
    },
    async getParserData(data, isall) {
      try {
        let result = await getParser(data);
        if (result.message) {
          Message.error(result.message);
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

        colLists =
          this.$store.state.app.currentDBType == "csv"
            ? result[0].fields
            : result[0].fields
                .map((item) => item.name)
                .filter((val) => {
                  if (
                    this.$store.state.app.currentDBType == "mqtt" &&
                    !this.mqttDefaultCols.includes(val)
                  ) {
                    return val;
                  }
                  if (
                    this.$store.state.app.currentDBType == "kafka" &&
                    !this.kafkaDefaultCols.includes(val)
                  ) {
                    return val;
                  }
                });
        tbdata = result[0].columns.map((data) => {
          return Object.fromEntries(
            result[0].fields.map((item, index) => {
              return [item.name, data[index] ? data[index].toString() : null];
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
          //获取全部的extract or split参数
          this.$store.commit("app/SET_TRANS_RESULT_TABLE", tbdata);
          return;
        }
        this.$store.commit(
          "app/SET_TRANSFORMER_MAPCOLUMNS",
          transformerColumns
        );
        this.tableColumns = colLists.map((item) => {
          let obj = {};
          let finalVal = tbdata.map(
            (val) =>
              val[
                this.$store.state.app.currentDBType == "csv" ? item.name : item
              ]
          );
          obj.name =
            this.$store.state.app.currentDBType == "csv" ? item.name : item;
          obj.value = finalVal.join("") ? finalVal.join(" ; ") : "";
          return obj;
        });
        // this.tableColumns = colLists;
        this.tableData = tbdata;
        this.$store.commit('app/SET_ACTIVE_COLS',Object.keys(tbdata[0]))
        let pageindex=this.tableData.findIndex(item=>item['Name']==Object.keys(tbdata[0])[0])
        this.$store.commit('app/SET_RESULT_PAGE',pageindex)
        // this.$store.commit("app/SET_TRANS_RESULT_TABLE", this.tableData);
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
      let extractExpres = this.ruleForm.filter_expres.split(";");
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
            Message.error(this.$t("datasource.transformer.jsontip"));
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
            }
          });
        return inputobj;
      });
      this.extractParseData = {
        extract: {},
      };
      deepClone(this.$parent.extractArr)
        .map((item) => {
          let splitobj = Object.fromEntries(
            Object.entries(item.splitParams).filter(([key, value]) => {
              return value !== null && value != undefined && value != "";
            })
          );
          splitobj["n"] = Number(splitobj["n"]);
          Object.hasOwnProperty.call(splitobj, "names")
            ? (splitobj["names"] = splitobj["names"].split(","))
            : splitobj;
          return {
            [`${item.columnname}`]: {
              [`${item.type}`]:
                item.type == "regex"
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
      let extractlist = {};
      topparse["parser"]["mutate"] = isall
        ? this.$store.state.app.transformerFilterParseData
          ? []
              .concat(this.$store.state.app.transformerFilterParseData)
              .concat(this.extractParseData)
          : [].concat(this.extractParseData)
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
          : inputList,
      };
      this.$store.commit("app/SET_EXTRACT_PARSE_DATA", this.extractParseData);
      this.$store.commit("app/SET_TRANS_RESULT_NAME", "");

      await this.getParserData(parser, isall);
    },
    deleteExtract() {
      this.$emit("deleteExtract", this.index, this.ruleForm.col_name);
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
};
</script>
<style lang="scss" scoped>
.extract-split {
  margin-top: 20px;
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
  margin-top: 10px;
  margin-bottom: 25px;
  display: grid;
  grid-template-columns: 1fr 1fr 1fr 1fr 1fr;
  column-gap: 15px;
  row-gap: 20px;
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
