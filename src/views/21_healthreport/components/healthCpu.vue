<template>
  <div class="health-cpu">
    <div class="flexEnd">
      <el-select v-model="day" placeholder="Please select" class="select-day" @change="changeDay">
        <el-option
          v-for="c in daysList"
          :key="c.value"
          :label="c.label"
          :value="c.value"
        >
        </el-option>
      </el-select>
      <el-button
        plain
        @click="refresh"
        size="small"
        icon="el-icon-refresh"
        :disabled="requestIng"
        >{{ $t("refresh") }}</el-button
      >
    </div>
    <el-table style="margin-top: 20px" :data="cpuData" size="mini">
      <!-- <el-table-column
        :label="$t('health.dnode_id')"
        prop="dnode_id"
      ></el-table-column> -->
      <el-table-column
        :label="$t('health.cpu_avg')"
        prop="cpu_avg"
      ></el-table-column>
      <el-table-column
        :label="$t('health.cpu_p99')"
        prop="cpu_p99"
      ></el-table-column>
      <el-table-column
        :label="$t('health.cpu_p90')"
        prop="cpu_p90"
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
  </div>
</template>
<script>
import { sendSQLReq } from "@/api/gateway/console";
import { Message } from "element-ui";
import mix from "./mix";
export default {
  name: "HealthCpu",
  mixins: [mix],
  data() {
    return {
      cpuData: [],
      requestIng: false,
    };
  },
  methods: {
    refresh() {
      this.getCpuData();
    },
    changeDay(){
        this.getCpuData()
    },
    async getCpuData() {
      try {
        this.requestIng = false;
        return await sendSQLReq(
          `select dnode_id, avg(cpu_engine) as cpu_avg, apercentile(cpu_engine, 90) as cpu_p90, apercentile(cpu_engine, 99) as cpu_p99 from log.dnodes_info where _c0 >= now - ${this.day}d partition by dnode_id;`
        ).then((res) => {
          this.cpuData = res.data.map((data) => {
            return Object.fromEntries(
              res.column_meta.map((item, index) => {
                if (item[0] !== "dnode_id") {
                  return [item[0], (Number(data[index]) * 100).toFixed(2) + "%"];
                } else {
                  return [item[0], data[index]];
                }
              })
            );
          });
          this.requestIng = false;
        });
      } catch (err) {
        this.requestIng = false;
        return Promise.reject(err);
      }
    },
  },
  created() {
    this.getCpuData();
  },
};
</script>
<style lang="scss" scoped>
::v-deep.el-select.select-day {
    margin-right:10px;
  .el-input__inner {
    height: 32px;
  }
}
</style>