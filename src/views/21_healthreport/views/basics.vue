<template>
  <div class="basic-operating">
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
    <el-table style="margin-top: 20px" :data="basicData" size="mini">
      <el-table-column
        :label="$t('health.expire_time')"
        prop="expire_time"
      ></el-table-column>
      <el-table-column
        :label="$t('health.uptime')"
        prop="uptime"
      ></el-table-column>
      <el-table-column
        :label="$t('health.version')"
        prop="version"
      ></el-table-column>
      <!-- <el-table-column :label="$t('topic.note')" prop="note"></el-table-column> -->

      <!-- <el-table-column label="Action" width="65">
        <template slot-scope="scope">
          <el-button
            plain
            size="small"
            @click="del(scope.row)"
            icon="el-icon-delete"
          ></el-button>
        </template>
      </el-table-column> -->
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
import { Message } from "element-ui";
export default {
  name: "BasicOperating",
  data() {
    return {
      basicData: [],
      currentPage:1,
      pageSize:10,
      total:0,
      requestIng:false
    };
  },
  methods: {
    handlePageChange(){},
    refresh(){
        this.getData()
    },
    async getData() {
      try {
        this.requestIng=true
        return await sendSQLReq(
          `select id, uptime, version, expire_time from information_schema.ins_cluster;`
        ).then((res) => {
          this.basicData = res.data.map((data) => {
            return Object.fromEntries(
              res.column_meta.map((item, index) => {
                return [item[0], data[index]];
              })
            );
          });
          this.requestIng=false
        });
      } catch (err) {
        this.requestIng=false
        return Promise.reject(err);
      }
    },
  },
  created() {
    this.getData();
  },
};
</script>