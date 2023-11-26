<template>
  <div class="filter-expression">
    <div class="filter-input">
      <el-form :model="ruleForm" :rules="rules" @submit.native.prevent ref="filterForm">
        <el-form-item prop="filter_name">
          <!-- <el-popover
            trigger="click"
            placement="top-start"
            :content="$t('datasource.transformer.mutiple')"
          >-->
          <el-input
            size="small"
            v-model="ruleForm.filter_name"
            :placeholder="$t('datasource.transformer.filter_input')"
            
            @input="changeFilterCont"
          ></el-input>
          <!-- </el-popover> -->
        </el-form-item>
      </el-form>

      <div class="btns">
        <el-button icon="el-icon-delete" @click="deleteFilter"></el-button>
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
import { getRFC3339Time } from "@/utils/index";
export default {
  name: "FilterExpression",
  props: {
    itemData: {
      type: Object,
      default: () => {
        return null;
      }
    },
    payload: {
      type: String,
      default: ""
    },
    inputparamsColumns: {
      type: Array,
      default: () => {
        return [];
      }
    },
    indentifiedColumns: {
      type: Array,
      default: () => {
        return [];
      }
    }
  },
  data() {
    return {
      maptypes: ["value", "generator", "join", "format", "sum", "expr"],
      ruleForm: {
        filter_name: ""
      },
      rules: {
        filter_name: [
          {
            required: true,
            trigger: "blur",
            message: this.$t("datasource.transformer.filter_input")
          }
        ]
      },
      tableData: []
    };
  },
  methods: {
    changeFilterCont(val) {
      this.$emit("changeFilter", this.itemData.key, val);
    },
    initData(val) {
      if (val) {
        this.ruleForm.filter_name = val.expression;
      }
    },
    submit() {
      this.$parent.validateMsgBody()
      if (!this.$parent.msgForm.msgbody) {
        return;
      }
      this.$refs.filterForm.validate(valid => {
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
        this.tableColumns = result[0].fields.map(item => item.name);
        if (result.message) {
          Message.error(result.message);
          return;
        }
        this.tableData = result[0].columns.map(data => {
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
            children: this.maptypes.map(item => {
              return {
                value: item,
                label: item
              };
            })
          },
          {
            value: "mapping",
            label: this.$t("mapping"),
            children: result[0].fields.map(item => {
              return {
                value: item.name,
                label: item.name
              };
            })
          }
        ];
        this.$store.commit(
          "app/SET_TRANSFORMER_MAPCOLUMNS",
          transformerColumns
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
      let inputobj = {};
      this.indentifiedColumns.forEach(item => {
        if (this.$store.state.app.currentDBType == "mqtt") {
          if (item.name == "payload") {
            inputobj["payload"] = this.payload;
          } else {
            inputobj[item.name] =
              item.type == "timestamp" ? getRFC3339Time() : item.name;
          }
        } else if (this.$store.state.app.currentDBType == "kafka") {
          if (item.name == "value") {
            inputobj["value"] = this.payload;
          } else {
            inputobj[item.name] =
              item.type == "timestamp" ? getRFC3339Time() : item.name;
          }
        }
      });
      let parser = {
        parser: {
          parse: {},
          mutate: [].concat({
            filter: this.ruleForm.filter_name
          })
        },
        input: [].concat(inputobj)
      };
      this.getParserData(parser);
    }
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
      }
    }
  }
};
</script>
<style lang="scss" scoped>
.filter-input {
  display: flex;
  align-items: center;
  margin-bottom: 20px;
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
        margin-right: 20px;
        margin-left: 20px;
      }
    }
  }
}
.table {
  margin-bottom: 20px;
}
</style>
