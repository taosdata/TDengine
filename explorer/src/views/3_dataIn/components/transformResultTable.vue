<template>
  <div class="result-table" v-if="showtable" ref="result" :style="{'max-height': resultTableMaxHeight +'px'}">
    <div class="title-block">
      <span class="title">{{ $t(`datasource.transformer.${title}`) }}</span>
      <span class="title-block">
        <el-tooltip placement="top" effect="light" :open-delay="0">
          <template slot="content">
            {{ $t('fullscreen') }}
          </template>
          <span class='el-icon-full-screen' @click="drawer=true"></span>
        </el-tooltip>
        <span class='el-icon-close' @click="showtable=false"></span>
      </span>
    </div>
    <template v-for="(tableItme, index) in pageTableData">
      <el-table
        :key="index"
        border
        style="width: 100%;margin-bottom: 20px;"
        :max-height="defaultHeight-99"
        :data="tableItme"
        :row-class-name="tableRowClassName"
        ref='table'
        v-if="!drawer"
      >
        <el-table-column
          v-for="item in columns[index]"
          :key="item"
          :prop="item"
          :sortable="item == 'Name' ? true : false"
          show-overflow-tooltip
          :label="item"
        >
        <template slot="header">
          <el-tooltip :content="item" placement="top-start">
            <span>{{ item }}</span>
          </el-tooltip>
        </template>
      </el-table-column>
      </el-table>
    </template>
    <el-drawer
      :title="$t(`datasource.transformer.${title}`)"
      :visible.sync="drawer"
      direction="rtl"
      size="100%">
      <template v-for="(tableItme, index) in pageTableData">
        <el-table
          :key="index"
          border
          style="width: 100%;margin-bottom: 20px;"
          :data="tableItme"
          :row-class-name="tableRowClassName"
          ref='table'
          size='small'
          v-if="drawer">
          <el-table-column
            v-for="item in columns[index]"
            :key="item"
            :prop="item"
            :sortable="item == 'Name' ? true : false"
            show-overflow-tooltip
            :label="item"
          >
          <template slot="header">
            <el-tooltip :content="item" placement="top-start">
              <span>{{ item }}</span>
            </el-tooltip>
          </template>
        </el-table-column>
        </el-table>
      </template>
    </el-drawer>
    <!-- <div class="block-page">
      <el-pagination
        :class="['pagination', totalCount < 10 ? 'hide' : '']"
        :page-size="pageSize"
        :current-page.sync="currentPage"
        layout="total,prev, pager, next, jumper"
        :total="totalCount"
        @current-change="handleCurrentChange"
      >
      </el-pagination>
    </div> -->
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
      loading: true,
      isFixed: false,
      columns: [],
      tableData: [],
      pageTableData: [],
      pageSize: 20,
      totalCount: 10,
      currentPage: 1,
      showtable: false,
      mqttDefaultCols: ["topic", "qos", "payload"],
      kafkaDefaultCols: ["topic", "partition", "offset", "key", "value"],
      MongoDBDefaultCols: ["value"],
      mappingCol: ["SubTableName", "SuperTableName"],
      defaultHeight:510,
      drawer: false,
      resultTableMaxHeight: 510
    };
  },
  mounted() {
    if (
      this.$store.state.app.transformresulttable.length > 0 &&
      !this.isEditable &&
      this.$store.state.app.transresultname
    ) {
      this.getResultData(this.$store.state.app.transformresulttable);

      this.handleScroll();
    }
    const mainDom = document.querySelector(".main_content");
    const parserDom = document.querySelector('#parser')
    this.$nextTick(()=>{
      let height=mainDom.offsetHeight
      this.defaultHeight=height-100
      this.resultTableMaxHeight = parserDom?.offsetHeight-200
    })
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
        let dom = document.querySelector(".block-title.top");
        if (dom) {
          const mainDom = document.querySelector(".main_content");

          const scrollTop = mainDom.scrollTop;
          let top = scrollTop >= dom.offsetTop ? scrollTop : dom.offsetTop;
          this.$store.commit("app/SET_TRANS_TABLE_HEIGHT", top);
          if (this.$refs.result) {
            if(this.$store.state.app.currentDBType=='csv'){
              const csvdom=document.querySelector(".csv-data")
              let csvtop = top >= (csvdom.offsetTop+dom.offsetTop) ? top : (csvdom.offsetTop+dom.offsetTop+25);
                this.$refs.result.style.top =csvtop +"px";
            }else{
              let commomtop=scrollTop >= dom.offsetTop ? scrollTop -160 : dom.offsetTop;
                this.$refs.result.style.top = commomtop + "px";
            }
          }
        }
      });
    },
    setPageTableData() {
      this.$set(
        this,
        "pageTableData",
        this.tableData
        // this.tableData.slice(
        //   (this.currentPage - 1) * this.pageSize,
        //   this.currentPage * this.pageSize
        // )
      );
    },
    handleCurrentChange(val) {
      this.currentPage = val;
      this.pageTableData.splice(0, Infinity);
      this.setPageTableData();
    },
    isArray2D(arr) {
      return Array.isArray(arr) && arr.length >0 && arr.every(item => Array.isArray(item));
    },
    getResultData(data) {
      let data2D = []
      if (this.isArray2D(data)) {
        data2D = data
      } else {
        data2D.push(data)
      }

      let hiddenCols = [];
      if (this.$store.state.app.currentDBType == "mqtt") {
        hiddenCols = this.mqttDefaultCols;
      }
      if (this.$store.state.app.currentDBType == "kafka") {
        hiddenCols = this.kafkaDefaultCols;
      }
      if (this.$store.state.app.currentDBType == "mongodb") {
        hiddenCols = this.MongoDBDefaultCols;
      }
      let columns = data2D.map(item => {
        return Object.keys(item[0]).filter((field) => {
          return !hiddenCols.includes(field);
        });
        
      })

      this.columns = columns;
      this.totalCount = columns.length;
      if (this.$store.state.app.resultTbTitle == 'mappingResTb') {
        this.mappingCol.forEach(item => {
          this.columns.forEach(cols => {
            const index = cols.indexOf(item);
            if (index > 0) {
              cols.splice(index, 1);
              cols.unshift(item);
            }
          })
        })

      }
      this.tableData = data2D.map(arr => arr.slice(0,this.limitOffset))

      const timer = setTimeout(() => {
        clearTimeout(timer)
        const targetRow =
          this.$store.state.app.activeColumns.length > 0
            ? document.querySelector("tr.el-table__row.active-row")
            : document.querySelector("tr.el-table__row");
        if (targetRow) {
          if (this.$store.state.app.activeColumns.length > 0) {
            const y = targetRow.offsetTop;
            this.$el?.querySelector(".el-table__body-wrapper").scrollTo(0, y);
          } else {
            this.$el?.querySelector(".el-table__body-wrapper").scrollTo(0, 0);
          }
        }
      }, 200);
      this.setPageTableData();
    },
  },
  computed: {
    title() {
      return this.$store.state.app.resultTbTitle
    },
    limitOffset() {
      // return this.$store.state.app.limitOffset
      return 100;
    }
  },
  watch: {
    // "$store.state.app.resultCurrentPage": {
    //   deep: true,
    //   handler(val) {
    //     if (val > 20) {
    //       this.handleCurrentChange(Math.floor(val / this.pageSize) + 1);
    //     } else {
    //       this.handleCurrentChange(1);
    //     }
    //   },
    // },
    "$store.state.app.showresulttb": {
      deep: true,
      handler(val, oldval) {
        this.showtable = val;
      },
    },
    "$store.state.app.transformresulttable": {
      deep: true,
      handler(val) {
        if (val && val.length > 0 && this.$store.state.app.transresultname) {
          this.showtable=true
          this.handleScroll();
          this.getResultData(val);
        } else {
          this.$set(this, "pageTableData", []);
          this.$set(this, "tableData", []);
          this.totalCount=0
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
  overflow-y: auto;
  .block-page {
    overflow: auto;
  }
  //   top: 54%;
  .title-block {
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    margin-bottom: 15px;
    .title {
      color: #4259ce;
      font-size: 14px;
      font-weight: 600;
    }
    .el-icon-close {
      cursor: pointer;
    }
    .el-icon-full-screen {
      cursor: pointer;
      display: inline-block;
      width: 30px;
    }
  }
  ::v-deep {
    .el-pagination__jump{
      display:none;
    }
    .pagination{
      margin-top:15px;
    }
    .el-table {
      thead tr th {
        background-color: #f5f7fa;
      }

      //   .el-table--border{
      //     border-color: transparent !important;
      //   }
      .el-table--group::after{
        border-color: transparent !important;
      }
      //   .el-table__column-resize-proxy {
      //     display: none !important;
      //   }
      &.el-table__cell {
        padding: 6px 0px!important;
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
