<template>
  <div class="filter-expression">
    <div class="filter-input">
      <el-form
        :model="ruleForm"
        :rules="rules"
        @submit.native.prevent
        ref="filterForm"
      >
        <el-form-item prop="filter_name">
          <!-- <el-popover
            trigger="click"
            placement="top-start"
            :content="$t('datasource.transformer.mutiple')"
          > -->
          <el-input
            size="small"
            v-model="ruleForm.filter_name"
            :placeholder="$t('datasource.transformer.filter_input')"
            @keyup.enter.native="excuteFilter"
            @input="changeFilterCont"
          ></el-input>
          <!-- </el-popover> -->
        </el-form-item>
      </el-form>

      <div class="btns">
        <el-button icon="el-icon-delete" @click="deleteFilter"></el-button>
        <el-button
          icon="el-icon-PREVIEW"
          @click="excuteFilter"
          style="display: flex"
        ></el-button>
      </div>
    </div>
    <!-- <div class='tip' v-if='ruleForm.filter_name'>
      <span :class="['excutetip',isexecuted?'done':'']">{{isexecuted?$t('datasource.transformer.filterexecuted'):$t('datasource.transformer.filterunexe')}}</span>
    </div> -->
  </div>
</template>
<script>
import { getParser } from "@/api/explorer/datain";
import { Message } from "element-ui";
import { parsinginZone } from "@/utils";
export default {
  name: "FilterExpression",
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
    inputparamsColumns: {
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
      isexecuted:false,
      maptypes: ["value", "generator", "join", "format", "sum", "expr"],
      ruleForm: {
        filter_name: "",
      },
      rules: {
        filter_name: [
          {
            required: false,
            trigger: "blur",
            message: this.$t("datasource.transformer.filter_input"),
          },
        ],
      },
      tableData: [],
    };
  },
  methods: {
    excuteFilter(){
      this.isexecuted=true
      this.submit()
    },
    changeFilterCont(val) {
      this.isexecuted=false
      // this.$emit("changeFilter", this.itemData.key, val);
      this.$store.commit("app/SET_FILTER_PARSE_DATA", {
        filter: this.ruleForm.filter_name,
      });
    },
    initData(val) {
      if (val) {
        this.ruleForm.filter_name = val.expression;
      }
    },
    submit() {
      this.$parent.validateMsgBody();
      if (!this.$parent.msgForm.msgbody) {
        return;
      }
      this.$refs.filterForm.validate((valid) => {
        if (valid) {
          this.submitFilter();
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
          this.$error(result.message);
          return;
        }
        this.$emit(
          "changeFilter",
          this.itemData.key,
          this.ruleForm.filter_name
        );
        result[0].columns?.length > 0 
        ? this.tableData = result[0].columns.map((data) => {
            return Object.fromEntries(
              result[0].fields.map((item, index) => {
                return [
                  item.name,data[index] ? data[index].toString() : null
                ];
              })
            );
          })
        : this.tableData = [].concat(
            Object.fromEntries(
              this.tableColumns.map((data) => {
                return [[data], null]
              })
            )
          )
        // if (!this.isViewable) {
          this.$store.commit('app/SET_RESULTTB_SHOW',true)
        // }
        this.$store.commit("app/SET_RESULTTB_TITLE_SHOW", 'filterResTb');
        this.$store.commit("app/SET_TRANS_RESULT_TABLE", this.tableData);
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
        this.$store.commit(
        "app/SET_TRANS_RESULT_NAME",
        'filter'
      );
      } catch (error) {
        console.log(error);
      }
    },
    //删除filter
    deleteFilter() {
      this.$emit("deleteFilter", this.itemData.key);
    },
    //提交
    submitFilter() {
      let inputList = [];
      let resultMsgbody = "";
      if (
        this.$parent.msgForm.msgbody.replace(/\}\s*\{/g, "}{").includes("}{")
      ) {
        resultMsgbody = this.$parent.msgForm.msgbody
          .replace(/\}\s*\{/g, "}&${")
          .split("&$");
      } else {
        if (
          /\n/g.test(this.$parent.msgForm.msgbody) &&
          /^[^\{]/.test(this.$parent.msgForm.msgbody.trim())
        ) {
          //普通文本，目前第一列暂时不能为json格式
          resultMsgbody = this.$parent.msgForm.msgbody
            .replace(/[\n\s]/g, "*&$*")
            .split("*&$*");
        } else {
          try {
            if (
              /^\{/g.test(this.$parent.msgForm.msgbody) &&
              JSON.parse(this.$parent.msgForm.msgbody)
            ) {
              resultMsgbody = [].concat(this.$parent.msgForm.msgbody);
            }
          } catch (error) {
            this.$error(this.$t("datasource.transformer.jsontip"));
            return;
          }

          resultMsgbody = this.$parent.msgForm.msgbody.split(";");
        }
      }
      inputList = resultMsgbody.map((msg) => {
        let inputobj = {};
        this.indentifiedColumns.forEach((item) => {
          if (this.$store.state.app.currentDBType == "mqtt") {
            if (item.name == "payload") {
              inputobj["payload"] = msg;
            } else {
              inputobj[item.name] =
                item.type == "timestamp"
                  ? parsinginZone(new Date())
                  : item.name;
            }
          } else if (this.$store.state.app.currentDBType == "kafka") {
            if (item.name == "value") {
              inputobj["value"] = msg;
            } else {
              inputobj[item.name] =
                item.type == "timestamp"
                  ? parsinginZone(new Date())
                  : item.name;
            }
          } else if (this.$store.state.app.currentDBType == "mongodb") {
            if (item.name == "value") {
              inputobj["value"] = msg;
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
      let parser = {
        parser: {
          parse: this.$store.state.app.topParse.parser.parse,
          mutate:this.$store.state.app.transformExtractParseData
             ? [].concat(this.$store.state.app.transformExtractParseData).concat({filter: this.ruleForm.filter_name.trim()})
             : [].concat({filter: this.ruleForm.filter_name.trim(),}),
        },
        input: this.$parent.isCSV
          ? this.$store.state.app.csvTransformerParser.inputList
          :this.$store.state.app.supportSQL?this.$store.state.app.topParse.input: inputList,
      };

      this.$store.commit("app/SET_FILTER_PARSE_DATA", {
        filter: this.ruleForm.filter_name,
      });
      this.isexecuted=true
      this.getParserData(parser);
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
<style lang="scss" scoped>
.filter-expression {
  margin-top: 10px;
  margin-bottom:20px;
}
.filter-input {
  display: flex;
  align-items: center;
  margin-bottom: 5px;
  .el-form {
    flex: 1;
  }
  .el-form-item {
    margin-bottom: 0px !important;
  }
  .btns {
    display: flex;
    .el-button {
      display: flex;
      align-items: center;
      justify-content: center;
      height: 32px;
      width: 32px;
      border-radius: 6px;
      &:first-child {
        margin-left: 20px;
      }
    }
  }
}
.table {
  margin-bottom: 20px;
}
.tip{
  font-size:12px;
  .excutetip{
    color:red;
    &.done{
      color:#acaab2;
    }
  }
}
</style>
