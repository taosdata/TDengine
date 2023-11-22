<template>
  <div class="filter-expression">
    <div class="filter-input">
      <el-form :model="ruleForm" :rules="rules">
        <el-form-item prop="filter_name">
          <el-input size="small" v-model="ruleForm.filter_name"></el-input>
        </el-form-item>
      </el-form>

      <div class="btns">
        <el-button icon="el-icon-delete" @click="deleteFilter"></el-button>
        <el-button icon="el-icon-check" @click="submitFilter"></el-button>
      </div>
    </div>

    <div class="table">
      <el-table :data="tableData" border style="width: 100%">
        <!-- <el-table-column
          v-for="(item, index) in tableColumns"
          :key="index"
          :label="tableColumns[index]"
          :prop="tableColumns[index]"
          show-overflow-tooltip
        ></el-table-column> -->
      </el-table>
    </div>
  </div>
</template>
<script>
import { getParser } from "@/api/explorer/datain";
export default {
  name: "FilterExpression",
  props: {
    index: {
      type: Number,
      default: 0,
    },
  },
  data() {
    return {
        ruleForm:{
            filter_name:''
        },
        rules:{
            filter_name:[
                {
                    required:true,
                    trigger:'blur',
                    message:this.$t('datasource.transformer.filter_input')
                }
            ]
        },
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

        console.log(result, this.tableData, "jieguo---结果--filter");
      } catch (error) {
        console.log(error);
      }
    },
    //删除filter
    deleteFilter() {
      this.$emit("deleteFilter", this.index);
    },
    //提交
    submitFilter() {
        console.log(this.ruleForm.filter_name,'filter的参数---9999')
    },

  },
};
</script>
<style lang="scss" scoped>
.filter-input {
  display: flex;
  align-items: center;
  margin-bottom: 20px;
  .el-form{
    flex:1;
  }
  .el-form-item{
    margin-bottom:0px!important;
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
