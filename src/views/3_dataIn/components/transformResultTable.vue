<template>
  <div class="result-table" v-if="showtable" ref='result'>
    <div class="title-block">
      <span class="title">{{
        $store.state.app.transresultname + $t("datasource.transformer.resulttb")
      }}</span>
      <!-- <span class='el-icon-close'></span> -->
    </div>
    <el-table border style="width: 100%" :data="pageTableData" :row-class-name="tableRowClassName">
      <el-table-column
        v-for="item in columns"
        :key="item"
        :prop="item"
        :sortable="item == 'Name' ? true : false"
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
  props: {
    isEditable: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
        isFixed:false,
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
    if (
      this.$store.state.app.transformresulttable.length > 0 &&
      !this.isEditable &&
      this.$store.state.app.transresultname
    ) {
      this.getResultData(this.$store.state.app.transformresulttable);
      this.showtable = true;
    }
    window.addEventListener('scroll',this.handleScroll)
  },
  destroy(){
    window.removeEventListener('scroll',this.handleScroll)
  },
  methods: {
    tableRowClassName({row,rowIndex}){
        if(this.$store.state.app.activeColumns.includes(row['Name'])){
            return 'active-row'
        }
        console.log(row,rowIndex,'kkkk---shezhi设置颜色',this.$store.state.app.activeColumns);
    },
    getOffsetTop(obj) {
      let offsettop = 0;
      while (obj != window.document.body && obj != null) {
        offsettop += obj.offsetTop;
        obj = obj.offsetParent;
      }
      return offsettop;
    },
    handleScroll() {
        let scrollTop=window.pageYOffset || document.documentElement.scrollTop||document.body.scrollTop
        let offsetTop=this.getOffsetTop(this.$refs.result)
        this.isFixed=scrollTop>offsetTop
        console.log(scrollTop,offsetTop,'高度');
    },
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
        //   this.showtable = true;
          this.getResultData(this.$store.state.app.transformresulttable);
          this.$nextTick(() => {
            let dom = document.querySelector(".result-table");
            if (dom) {
              dom.style.top = this.$store.state.app.transformTableHeight + "px";
            }
          });
        } else {
        //   this.showtable = false;
          this.$store.commit("app/SET_TRANS_RESULT_TABLE", []);
        }
      },
    },
    "$store.state.app.transformresulttable": {
      deep: true,
      handler(val) {
        this.showtable = true;
        if (val && val.length > 0 && this.$store.state.app.transresultname) {
            
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
  ::v-deep {
    .el-table .el-table__cell {
      padding: 6px 0px;
    }
    .el-table .active-row {
    background: #ecf2fe;
  }
  }
 
}
</style>
