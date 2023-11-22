<template>
  <div class="extract-split">
    <div class="extract-item">
      <el-form :model="ruleForm" :rules="rules" size="small">
        <el-form-item prop="col_name">
          <el-select
            size="small"
            :placeholder="$t('datasource.transformer.col_select')"
            v-model="ruleForm.col_name"
          >
            <el-option
              v-for="(item, index) in extractColumns"
              :key="index"
              :label="item.name"
              :value="item.name"
            ></el-option>
          </el-select>
        </el-form-item>
        <el-form-item prop="filter_name">
          <el-select
            size="small"
            :placeholder="$t('datasource.transformer.filter_type')"
            v-model="ruleForm.filter_name"
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
          <el-input
            size="small"
            :placeholder="$t('datasource.transformer.expre_input')"
            v-model="ruleForm.filter_expres"
          ></el-input>
        </el-form-item>
      </el-form>

      <div class="btns">
        <el-button icon="el-icon-delete" @click="deleteExtract"></el-button>
        <el-button icon="el-icon-check" @click="submitExtract"></el-button>
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
export default {
  name: "ExtractSplit",
  props: {
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
  },
  data() {
    return {
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
    async getParserData(data) {
      try {
        let result = await getParser(data);
        this.tableColumns = result[0].fields.map((item) => item.scope);
        this.tableData = result[0].columns.map((data) => {
          return Object.fromEntries(
            result[0].fields.map((item, index) => {
              return [item.scope, data[index]];
            })
          );
        });

        console.log(result, this.tableData, "jieguo---结果");
      } catch (error) {
        console.log(error);
      }
    },
    //提交单个
    submitExtract() {
      let parser = {
        parser: {
          parse: {
            [`${this.ruleForm.col_name}`]: {
              [`${this.ruleForm.filter_name}`]: [
                `${this.ruleForm.filter_expres}`,
              ],
            },
          },
        },
        input: [
          {
            [`${this.ruleForm.col_name}`]: "",
            payload: "{" + `${this.ruleForm.filter_expres}` + "}",
          },
        ],
      };
      this.getParserData(parser);
      console.log(parser, "提交单个");
    },
    deleteExtract() {
      this.$emit("deleteExtract", this.index);
    },
  },
  mounted() {
    console.log(this.extractColumns, "extractColumnsextractColumns");
  },
  watch: {
    extractColumns: {
      deep: true,
      handler(val) {
        console.log(val, "舰艇");
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
