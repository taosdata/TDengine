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
          <el-popover
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

      <div class="btns">
        <el-button icon="el-icon-delete" @click="deleteExtract"></el-button>
        <el-button icon="el-icon-check" @click="submit"></el-button>
      </div>
    </div>
    <div class="table" v-if="tableData.length > 0">
      <el-table :data="tableData" border style="width: 100%">
        <el-table-column
          v-for="(item, index) in tableColumns"
          :key="index"
          :label="tableColumns[index]"
          :prop="tableColumns[index]"
          show-overflow-tooltip
        ></el-table-column>
      </el-table>
    </div>
  </div>
</template>
<script>
import { getParser } from "@/api/explorer/datain";
import { Message } from "element-ui";
export default {
  name: "ExtractSplit",
  props: {
    itemData: {
      type: Object,
      default: () => {
        return null;
      },
    },
    payload: {
      type: String,
      default: "",
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
      extractParseData: {},
      maptypes: ["value", "generator", "join", "format", "sum", "expr"],
      tableColumns: [],
      extractTypes: ["json", "split", "regex"],
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
    changeExtractExpr(val) {
      this.$emit("changeExtractExpr", this.ruleForm.col_name, val);
    },
    initData(val) {
      this.ruleForm.col_name = val.columnname;
      this.ruleForm.filter_expres = val.expression;
      this.ruleForm.filter_name = val.type;
    },
    selectCol(data) {
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
      this.$refs.extractForm.validate((valid) => {
        if (valid) {
          this.submitExtract();
          return true;
        } else {
          return false;
        }
      });
    },
    async getParserData(data) {
      try {
        let result = await getParser(data);
        this.tableColumns = result[0].fields.map((item) => item.name);
        if (result.message) {
          Message.error(result.message);
          return;
        }
        this.tableData = result[0].columns.map((data) => {
          return Object.fromEntries(
            result[0].fields.map((item, index) => {
              return [item.name, data[index]];
            })
          );
        });
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
        this.$store.commit(
          "app/SET_TRANSFORMER_MAPCOLUMNS",
          transformerColumns
        );
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
    submitExtract() {
      let extractExpres = this.ruleForm.filter_expres.split(";");
      let inputList=[]
      inputList=this.$parent.msgForm.msgbody.split(";").map((msg) => {
        let inputobj = {};
        this.indentifiedColumns.forEach((item) => {
          if (this.$store.state.app.currentDBType == "mqtt") {
            if (item.name == "payload") {
              inputobj["payload"] = msg;
            } else {
              inputobj[item.name] =
                item.type == "timestamp" ? "now" : item.name;
            }
          } else if (this.$store.state.app.currentDBType == "kafka") {
            if (item.name == "value") {
              inputobj["value"] = msg;
            } else {
              inputobj[item.name] =
                item.type == "timestamp" ? "now" : item.name;
            }
          }
        });
        return inputobj
      });
      this.extractParseData = {};
      this.$parent.extractArr
        .map((item) => {
          return {
            [`${item.columnname}`]: {
              [`${item.type}`]:
                item.type == "regex"
                  ? item.expression
                  : item.expression.split(";"),
            },
          };
        })
        .forEach((val) => {
          Object.assign(this.extractParseData, val);
        });
      let parser = {
        parser: {
          parse: {},
        },
        input: inputList,
      };
      parser.parser.parse = this.extractParseData;
      this.$store.commit("app/SET_EXTRACT_PARSE_DATA", this.extractParseData);

      this.getParserData(parser);
    },
    deleteExtract() {
      this.$emit("deleteExtract", this.index, this.ruleForm.col_name);
    },
  },
  mounted() {
    if (this.itemData) {
      this.initData(this.itemData);
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
<style>
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
</style>
