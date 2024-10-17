<template>
  <div class="memory">
    <div class="flexEnd">
      <el-select
        v-model="day"
        placeholder="Please select"
        class="select-day"
        @change="changeDay"
      >
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
    <el-table style="margin-top: 20px" :data="memData" size="mini">
      <el-table-column
        :label="$t('health.mem_avg')"
        prop="mem_avg"
      ></el-table-column>
      <el-table-column
        :label="$t('health.mem_p99')"
        prop="mem_p99"
      ></el-table-column>
      <el-table-column
        :label="$t('health.mem_p90')"
        prop="mem_p90"
      ></el-table-column>
    </el-table>
  </div>
</template>
<script>
import { sendSQLReq } from "@/api/gateway/console";
import { Message } from "element-ui";
import mix from "./mix";
export default {
  name: "HealthMemory",
   mixins:[mix],
  data() {
    return {
      memData: [],
      requestIng: false,
    };
  },
  methods: {
    refresh() {
      this.getMemData();
    },
    changeDay(){
        this.getMemData()
    },
    //内存单位换算
    convertUnit(val) {
      if (val < 1000) {
        return val + "K";
      } else if (val > 1000 && val < 1000 * 1000) {
        return (val / 1000).toFixed(2) + "M";
      } else {
        return (val / (1000 * 1000)).toFixed(2) + "G";
      }
    },
    async getMemData() {
      try {
        this.requestIng = true;
        return await sendSQLReq(
          `select dnode_id, avg(mem_engine) as mem_avg, apercentile(mem_engine, 90) as mem_p90, apercentile(mem_engine, 99) as mem_p99 from log.dnodes_info where _c0 >= now - ${this.day}d partition by dnode_id;`
        ).then((res) => {
          this.memData = res.data.map((data) => {
            return Object.fromEntries(
              res.column_meta.map((item, index) => {
                if (item[0] !== "dnode_id") {
                  return [item[0], this.convertUnit(data[index])];
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
    this.getMemData();
  },
};
</script>
<style lang="scss" scoped>
::v-deep.el-select.select-day {
  margin-right: 10px;
  .el-input__inner {
    height: 32px;
  }
}
</style>