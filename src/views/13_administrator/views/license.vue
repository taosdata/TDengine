<template>
  <div class="dnode-block">
    <div class="flexEnd">
      <el-button
        plain
        @click="add"
        size="small"
        >{{ $t("taosuser.activationLicense") }}</el-button
      >
    </div>
    
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
    <el-table style="margin-top: 20px" :data="tableData" size="mini">
      <el-table-column
        :label="$t('topic.type')"
        prop="type"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.number')"
        prop="number"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.speed')"
        prop="speed"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.expire_time')"
        prop="expire"
      >
      <template slot-scope="scope">
        <span>{{ expireTime(scope.row.expire) }}</span>
      </template>
    </el-table-column>
    </el-table>
    <el-pagination
      class="pagination"
      layout="total, prev, pager, next"
      :current-page.sync="currentPage"
      :page-size="pageSize"
      :hide-on-single-page="true"
      :total="total"
      @current-change="handlePageChange"
    ></el-pagination>
    <el-dialog
      align="center"
      :title="$t('taosuser.activationLicense')"
      width="600px"
      :visible.sync="dialog"
      :destroy-on-close='true'
    >
      <el-form
        :model="ruleForm"
        :rules="rules"
        ref="ruleForm"
        size="mini"
        label-width="120px"
        class="demo-ruleForm"
      >
        <el-form-item
          :label="$t('taosuser.activeCode')"
          prop="active_code"
        >
          <el-input v-model.trim="ruleForm.active_code"></el-input>
        </el-form-item>
        <el-form-item
          :label="$t('taosuser.cActiveCode')"
          prop="c_active_code"
        >
          <el-input v-model.trim="ruleForm.c_active_code "></el-input>
        </el-form-item>
      </el-form>

      <el-row style="margin-top: 20px">
        <el-col :span="5" :offset="6">
          <el-button size="small" @click="dialog = false" class="w100">{{
            $t("cancel")
          }}</el-button>
        </el-col>
        <el-col :span="5" :push="4">
          <el-button
            size="small"
            :disabled="confirmStatus"
            @click="submit"
            class="w100"
            type="primary"
            >{{ $t("confirm") }}</el-button
          >
        </el-col>
      </el-row>
    </el-dialog>
  </div>
</template>
<script>
import moment from "moment";
import { sendSQLReq } from "@/api/gateway/console";
import { activeLicence } from '@/api/explorer/licence'
export default {
  data() {
    return {
      pageSize: 10,
      currentPage: 1,
      total: 10,
      dialog: false,
      loading: false,
      ruleForm: {
        active_code: "",
        c_active_code: "",
      },
      rules: {
        active_code: [
          {
            message: this.$t('dataIn.enterTip'),
          },
        ],
        c_active_code: [
          {
            message: this.$t('dataIn.enterTip'),
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
      if (!this.ruleForm.active_code && !this.ruleForm.c_active_code) {
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
          let allLicence =array.length>0?Object.keys(array[0]).map(key=>{
            return {
              key:key,
              value:array[0][key]
            }
          }):[]
          this.licenseList = allLicence.filter(item => item.value.indexOf('{') == -1)
          this.tableData = allLicence.filter(item => item.value.indexOf('{') == 0).map(data => {
            return JSON.parse(data.value)
          })
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
    add() {
      this.dialog = true
    },
    async submit() {
      try {
        await activeLicence(this.ruleForm).then(res => {
          console.log('res',res);
          this.$message.success(this.$t('operateSucc'))
        })     
      } catch (error) {
        this.$message.error(error)
      }
    }, 
    expireTime(data){
      return moment(Number(data)).format("YYYY-MM-DD")
    }
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