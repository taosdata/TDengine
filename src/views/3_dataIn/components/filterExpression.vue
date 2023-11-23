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
          <el-popover
            trigger="click"
            placement="right-end"
            :content="$t('datasource.transformer.mutiple')"
          >
            <el-input
              size="small"
              v-model="ruleForm.filter_name"
              :placeholder="$t('datasource.transformer.filter_input')"
              slot="reference"
              @input="changeFilterCont"
            ></el-input>
          </el-popover>
        </el-form-item>
      </el-form>

      <div class="btns">
        <el-button icon="el-icon-delete" @click="deleteFilter"></el-button>
        <el-button icon="el-icon-check" @click="submitFilter"></el-button>
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
  },
  data() {
    return {
      ruleForm: {
        filter_name: "",
      },
      rules: {
        filter_name: [
          {
            required: true,
            trigger: "blur",
            message: this.$t("datasource.transformer.filter_input"),
          },
        ],
      },
      tableData: [],
    };
  },
  methods: {
    changeFilterCont(val){
      this.$emit('changeFilter',this.itemData.key,val)
        console.log(val,'输入筛选条件')
    },
    initData(val) {
      if (val) {
        this.ruleForm.filter_name = val.expression;
      }
    },
    submit() {
      if (!this.$parent.msgbody) {
        Message.error(this.$t("datasource.transformer.msgbodytip"));
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
        console.log(result, "获取filter");
        this.tableColumns = result[0].fields.map((item) => item.name);
        if(result.message){
            Message.error(result.message)
            return
        }
        this.tableData = result[0].columns.map((data) => {
          return Object.fromEntries(
            result[0].fields.map((item, index) => {
              return [item.name, data[index]];
            })
          );
        });

        console.log(result, this.tableData, "jieguo---结果--filter");
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
      let filterExpres = this.ruleForm.filter_name.split(";");
      console.log(filterExpres, "拆分得filter");
      let parser = {
        parser: {
          parse: {},
          mutate: filterExpres.map((val) => {
            return {
              filter: val,
            };
          }),
        },
        input: this.inputparamsColumns.map((item) => {
          let obj = {};
          if (this.$store.state.app.currentDBType == "mqtt") {
            //mqtt
            if (item.name == "payload") {
              obj["payload"] = this.payload;
            } else {
              obj[item.name] = item.name;
            }
          } else if (this.$store.state.app.currentDBType == "kafka") {
            if (item.name == "value") {
              obj["value"] = this.payload;
            } else {
              obj[item.name] = item.name;
            }
          }
          return obj;
        }),
      };
      console.log(parser, "这是要穿的参数");
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
