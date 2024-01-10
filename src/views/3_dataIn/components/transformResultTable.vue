<template>
  <div class="result-table" v-if="showtable" ref="result">
    <div class="title-block">
      <span class="title">{{ $t("datasource.transformer.resulttb") }}</span>
      <!-- <span class='el-icon-close'></span> -->
    </div>
    <el-table
      border
      style="width: 100%"
      max-height="600"
      :data="pageTableData"
      :row-class-name="tableRowClassName"
    >
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
        :current-page.sync="currentPage"
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
      isFixed: false,
      columns: ["Name", "Output1", "Output2", "Output3"],
      tableData: [],
      pageTableData: [],
      pageSize: 20,
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
      this.handleScroll();
    }
    const mainDom = document.querySelector(".main_content");
    mainDom.addEventListener("scroll", this.handleScroll);
    this.$once("hook:beforeDestroy", () => {
      mainDom.removeEventListener("scroll", this.handleScroll);
    });
  },
  methods: {
    tableRowClassName({ row, rowIndex }) {
      if (this.$store.state.app.activeColumns.includes(row["Name"])) {
        return "active-row";
      }
    },
    handleScroll() {
      this.$nextTick(() => {
        let dom = document.querySelector(".transdescription");
        if (dom) {
          const mainDom = document.querySelector(".main_content");
          const scrollTop = mainDom.scrollTop;
          let top = scrollTop >= dom.offsetTop ? scrollTop : dom.offsetTop;
          this.$store.commit("app/SET_TRANS_TABLE_HEIGHT", top);
          if (this.$refs.result) {
            this.$refs.result.style.top = top - 200 + "px";
            // this.$refs.result.style.bottom=70+'px'
          }
        }
      });
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
      let hiddenCols = [];
      if (this.$store.state.app.currentDBType == "mqtt") {
        hiddenCols = this.mqttDefaultCols;
      }
      if (this.$store.state.app.currentDBType == "kafka") {
        hiddenCols = this.kafkaDefaultCols;
      }
      let columns = Object.keys(data[0]).filter((item) => {
        return !hiddenCols.includes(item);
      });
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
      this.$nextTick(() => {
        const targetRow = document.querySelector("tr.el-table__row.active-row");
        if (targetRow) {
            const bound = targetRow.getBoundingClientRect()
            const y = bound.y
            this.$el.querySelector('.el-table__body-wrapper').scrollTo(0, y)
        }
      });
      this.setPageTableData();
    },
  },
  watch: {
    "$store.state.app.resultCurrentPage": {
      deep: true,
      handler(val) {
        if (val > 20) {
          this.handleCurrentChange(Math.floor(val / this.pageSize) + 1);
        } else {
          this.handleCurrentChange(1);
        }
      },
    },
    "$store.state.app.transformresulttable": {
      deep: true,
      handler(val) {
        this.showtable = false;
        if (val && val.length > 0 && this.$store.state.app.transresultname) {
          this.showtable = true;
          this.handleScroll();
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
    .el-table {
      thead tr th {
        background-color: #f5f7fa;
      }

      //   .el-table--border{
      //     border-color: transparent !important;
      //   }
      .el-table--group::after,
      .el-table--border::after,
      .el-table::before {
        border-color: transparent !important;
      }
      //   .el-table__column-resize-proxy {
      //     display: none !important;
      //   }
      &.el-table__cell {
        padding: 6px 0px;
      }
      .active-row {
        background: #ecf2fe !important;
      }
      &::before {
        background-color: transparent;
      }
    }
  }
}
</style>
