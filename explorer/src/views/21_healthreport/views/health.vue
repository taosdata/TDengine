<template>
  <div class="health">
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
    <el-table style="margin-top: 20px" :data="healthData" size="mini">
      <el-table-column
        :label="$t('health.elapsed(ts, 1m)')"
        prop="elapsed(ts, 1m)"
      ></el-table-column>
    </el-table>
  </div>
</template>
<script>
import { sendSQLReq } from "@/api/gateway/console";
import { Message } from "element-ui";
export default {
  name: "Health",
  data() {
    return {
      healthData: [],
      requestIng:false
    };
  },
  methods: {
    refresh(){
        this.getHealthData()

    },
    async getHealthData() {
      try {
        this.requestIng=true
        return await sendSQLReq(
          `select ELAPSED(ts, 1m) from log.cluster_info;`
        ).then((res) => {
          this.healthData = res.data.map((data) => {
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
    this.getHealthData();
  },
};
</script>