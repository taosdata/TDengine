<template>
  <div class="dnode-block">
    <div class="flexEnd">
      <el-button
        plain
        @click="refresh"
        size="small"
        icon="el-icon-refresh"
        :disabled="requestIng"
        >{{ $t("refresh") }}</el-button
      >
    </div>
    <el-table style="margin-top: 20px" :data="auditList" size="mini">
      <el-table-column :label="$t('taosuser.time')" prop="ts" width="220">
        <span slot-scope="scope">{{ parsinginZone(scope.row.ts) }}</span>
      </el-table-column>
      <el-table-column :label="$t('taosuser.users')" prop="user_name">
      </el-table-column>
      <el-table-column :label="$t('taosuser.operation')" prop="operation">
      </el-table-column>
      <el-table-column :label="$t('taosuser.details')" prop="details" width="280" :show-overflow-tooltip="true">
      </el-table-column>
      <el-table-column :label="$t('taosuser.target_1')" prop="target_1" :show-overflow-tooltip="true">
      </el-table-column>
      <el-table-column :label="$t('taosuser.target_2')" prop="target_2" :show-overflow-tooltip="true">
      </el-table-column>
      <el-table-column :label="$t('topic.clusterId')" prop="cluster_id" :show-overflow-tooltip="true">
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
  </div>
</template>
<script>
import { sendSQLReq } from "@/api/gateway/console";
import { getAudits } from '@/api/explorer/audit'
import { Message } from "element-ui";
import { getDBListReq } from "@/api/gateway/data/dbs.js";
import { parsinginZone } from '@/utils'
export default {
  data() {
    return {
      requestIng: false,
      dblist: [],
      pageSize: 10,
      currentPage: 1,
      total: 10,
      auditList: [],
      parsinginZone
    };
  },
  computed: {
  },
  methods: {
    handlePageChange() {
      this.getAuditData()
    },
    refresh() {
      this.getAuditData();
    },
    async getAuditData() {
      try {
        this.requestIng = true;
        [ this.auditList, this.total ]= await getAudits({ currentPage: this.currentPage, pageSize: this.pageSize })
        
        console.log('permissionMap',this.auditList);
        this.requestIng = false;
      } catch (error) {
        console.log('err');
      }
    },
    async getDatabases() {
      try {
        this.dblist = await getDBListReq();
      } catch (err) {
        return Promise.reject(err);
      }
    },
  },
  created() {
    this.getDatabases();
    this.getAuditData();
  },
};
</script>
<style lang="scss" scoped>
.el-select {
  width: 100%;
}
.el-switch {
  margin-right: 10px;
}
</style>
