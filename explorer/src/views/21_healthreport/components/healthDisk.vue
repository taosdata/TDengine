<template>
  <div class="disk">
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
    <el-table style="margin-top: 20px" :data="diskData" size="mini">
      <el-table-column
        :label="$t('health.max(disk_engine)')"
        prop="max(disk_engine)"
      ></el-table-column>
      <el-table-column
        :label="$t('health.last(disk_engine)')"
        prop="last(disk_engine)"
      ></el-table-column>
    </el-table>
  </div>
</template>
<script>
import { sendSQLReq } from "@/api/gateway/console";
import { Message } from "element-ui";
import mix from "./mix";
export default {
  name: "HealthDisk",
  mixins:[mix],
  data() {
    return {
      diskData: [],
      requestIng: false,
    };
  },
  methods: {
    refresh() {
      this.getDiskData();
    },
    changeDay(){
        this.getDiskData()
    },
    async getDiskData() {
      try {
        this.requestIng = true;
        return await sendSQLReq(
          `select dnode_id, max(disk_engine), last(disk_engine) from log.dnodes_info where _c0 >= now - ${this.day}d partition by dnode_id;`
        ).then((res) => {
          this.diskData = res.data.map((data) => {
            return Object.fromEntries(
              res.column_meta.map((item, index) => {
                if(item[0] !== "dnode_id"){
                    return [item[0], data[index]+' bytes'];
                }else{
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
    this.getDiskData();
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