<template>
  <div class="dnode-block">
    <!-- <div class="flexEnd">
      <el-button
        plain
        @click="refresh"
        size="small"
        icon="el-icon-refresh"
        :disabled="loading"
        >{{ $t("refresh") }}</el-button
      >
    </div> -->
    <!-- <el-table style="margin-top: 20px" :data="licenseList" size="mini">
      <el-table-column
        :label="$t('topic.accounts')"
        prop="accounts"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.connections')"
        prop="connections"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.cpu_cores')"
        prop="cpu_cores"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.databases')"
        prop="databases"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.dnodes')"
        prop="dnodes"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.expire_time')"
        prop="expire_time"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.expired')"
        prop="expired"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.querytime')"
        prop="querytime"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.speed')"
        prop="speed"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.storage')"
        prop="storage"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.streams')"
        prop="streams"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.timeseries')"
        prop="timeseries"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.users')"
        prop="users"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.version')"
        prop="version"
      ></el-table-column>
    </el-table> -->
    <!-- <el-table :data="tableData" :show-header="false" border>
      <el-table-column prop="header" label="表头"> </el-table-column>
      <el-table-column
        v-for="(item, index) in columns"
        :key="index"
        :prop="String(index)"
      >
      </el-table-column>
    </el-table> -->
    <el-descriptions
      class="margin-top"
      title=""
      :column="3"
    >
      <el-descriptions-item v-for="item in licenseList" :key="item.key" :label='$t(`topic.${item.key}`)' :labelStyle='style'>
        <span style="color:#333;"> {{item.value}}</span>
      </el-descriptions-item>
    </el-descriptions>
    <el-pagination
      class="pagination"
      layout="total, prev, pager, next"
      :current-page.sync="currentPage"
      :page-size="pageSize"
      :hide-on-single-page="true"
      :total="total"
      @current-change="handlePageChange"
    ></el-pagination>
   
  </div>
</template>
<script>
import { sendSQLReq } from "@/api/gateway/console";
export default {
  data() {
    return {
      pageSize: 10,
      currentPage: 1,
      total: 10,
      dialog: false,
      loading: false,
      ruleForm: {
        name: "",
        language: "",
        content: "",
      },
      rules: {
        name: [
          {
            message: "Please enter the name",
            trigger: "blur",
          },
        ],
        language: [
          {
            message: "Please select the language",
            trigger: "change",
          },
        ],
        content: [
          {
            message: "Please enter the content",
            trigger: "blur",
          },
        ],
      },
      licenseList: [],
      columns: [],
      tableData: [],
    };
  },
  computed: {
    style(){
      return {
        'font-size':'14px',
        'color':'#4d6992'
      }
    },
    confirmStatus() {
      if (!this.ruleForm.name) {
        return true;
      }
      if (!this.ruleForm.language) {
        return true;
      }
      if (!this.ruleForm.content) {
        return true;
      }
      return false;
    },
  },
  created() {
    this.getData();
  },
  methods: {
    handlePageChange() {},
    del(data) {
      this.$confirm("Are you sure  to delete " + data.name + "?", "Warning", {
        confirmButtonText: "Ok",
        cancelButtonText: "Cancel",
        type: "warning",
      });
    },
    refresh() {
      this.loading = true;
      this.getData();
    },
    addUdf() {},
    async getData() {
      try {
        // let cols = [];
        await sendSQLReq(`show grants;`).then((res) => {
         let  array = res.data.map((data) => {
            return Object.fromEntries(
              res.column_meta.map((item, index) => {
                // cols.push({ header: item[0], value: item[0] });
                return [item[0], data[index]];
              })
            );
          });
          this.licenseList=array.length>0?Object.keys(array[0]).map(key=>{
            return {
              key:key,
              value:array[0][key]
            }
          }):[]
          // this.columns = new Array(this.licenseList.length).fill(0);
          // this.tableData=JSON.parse(JSON.stringify(cols))
          // const tableData = cols.map((item) => {
          //   const data = {
          //     header: item.header,
          //   };
          //   this.licenseList.forEach((col, index) => {
          //     data[index] = col[item.value];
          //   });
          //   return data;
          // });
          // this.tableData = tableData;
        });
        this.loading = false;
      } catch (error) {
        this.loading = false;
      }
    },
  },
};
</script>
<style lang="scss" scoped>
.dnode-block{
  margin-top:10px;
}
::v-deep {
  .el-form-item__content {
    display: flex;
  }
  .el-select.el-select--mini {
    flex: 1;
  }
  tr.el-table__row {
    td {
      &:first-child {
        background: #fafafa;
        color: #333;
        font-weight: 500;
      }
    }
  }
th.el-descriptions-item__cell.el-descriptions-item__label.is-bordered-label{
  width:80px;
}
td.el-descriptions-item__cell.el-descriptions-item__content{
  width:200px;
}
.el-descriptions .el-descriptions-item__cell{
  padding:12px 5px;
  border-bottom: 1px solid #dfe6ec;
}
}
</style>