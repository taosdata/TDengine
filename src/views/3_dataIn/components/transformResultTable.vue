<template>
  <div class="result-table" v-if="showtable">
    <div class="title-block">
      <span class="title">{{
        $store.state.app.transresultname + $t("datasource.transformer.resulttb")
      }}</span>
      <!-- <span class='el-icon-close'></span> -->
    </div>
    <el-table border style="width: 100%" :data="pageTableData">
      <el-table-column
        v-for="item in columns"
        :key="item"
        :prop="item"
        show-overflow-tooltip
        :label="item"
      ></el-table-column>
    </el-table>
    <div class="block-page">
      <el-pagination
        :class="['pagination', totalCount < 10 ? 'hide' : '']"
        :page-size="pageSize"
        layout="total,prev, pager, next, jumper"
        :total="totalCount"
        @current-change="handleCurrentChange"
      >
      </el-pagination>
    </div>
  </div>
</template>
<script>
export default {
  name: "ResultTable",
  props:{
    isEditable: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      columns: ["Name", "Output1", "Output2", "Output3"],
      tableData: [],
      pageTableData: [],
      pageSize: 10,
      totalCount: 10,
      currentPage: 1,
      showtable: false,
      mqttDefaultCols: ["topic", "qos", "payload"],
      kafkaDefaultCols: ["topic", "partition", "offset", "key", "value"],
    };
  },
  mounted() {
    if (this.$store.state.app.transformresulttable.length > 0&&!this.isEditable&&this.$store.state.app.transresultname) {
      this.getResultData(this.$store.state.app.transformresulttable);
      this.showtable = true;
    }
  },
  methods: {
    setPageTableData() {
      this.$set(
        this,
        "pageTableData",
        this.tableData.slice(
          (this.currentPage - 1) * this.pageSize,
          this.currentPage * this.pageSize
        )
      );
    },
    handleCurrentChange(val) {
      this.currentPage = val;
      this.pageTableData.splice(0, Infinity);
      this.setPageTableData();
    },
    getResultData(data) {
      let totalData = [];
      let columns = Object.keys(data[0]);
      this.totalCount = columns.length;
      this.tableData = columns
        .map((key) => {
          let obj = {};
          obj["Name"] = key;
          obj["Value"] = data
            .map((val) => {
              return val[key];
            })
            .join(";");
          return obj;
        })
        .map((val) => {
          let final = {};
          final["Output1"] = null;
          final["Output2"] = null;
          final["Output3"] = null;
          final["Name"] = val["Name"];
          val["Value"].split(";").map((v, ind) => {
            final["Output" + (ind + 1)] = v;
          });
          return final;
        });
      this.setPageTableData();
    },
  },
  watch: {
    "$store.state.app.transresultname": {
      deep: true,
      handler(val) {
        if (val) {
          this.showtable = true;
          this.getResultData(this.$store.state.app.transformresulttable);
          this.$nextTick(() => {
            let dom = document.querySelector(".result-table");
            if (dom) {
              dom.style.top = this.$store.state.app.transformTableHeight + "px";
            }
          });
        }else{
            this.showtable = false
            this.$store.commit("app/SET_TRANS_RESULT_TABLE", []);
        }
      },
    },
    "$store.state.app.transformresulttable": {
      deep: true,
      handler(val) {
        if (val && val.length > 0&&this.$store.state.app.transresultname) {
            this.getResultData(val);
        }
      },
    },
  },
};
</script>
<style>
/* @media screen and (max-width: 1366px) {
  .result-table {
    background: red;
    display: none !important;
  }
} */
</style>
<style lang="scss" scoped>
.result-table {
  border: 1px solid #e3e4e6;
  border-radius: 12px;
  padding: 20px;
  width: 100%;
  //   max-width: 600px;
  //   min-width: 480px;
  position: absolute;
  .block-page {
    overflow: auto;
  }
  //   top: 54%;
  .title-block {
    display: flex;
    justify-content: space-between;
    margin-bottom: 15px;
    .title {
      color: #4259ce;
      font-size: 14px;
      font-weight: 600;
    }
    .el-icon-close {
      cursor: pointer;
    }
  }
}
</style>
