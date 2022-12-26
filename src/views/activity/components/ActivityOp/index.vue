<template>
  <div class="activity_op">
    <div>
      <el-date-picker
        v-model="date"
        size="small"
        type="datetimerange"
        :picker-options="$root.pickerOptions"
        range-separator="-"
        :start-placeholder="$t('start')"
        :end-placeholder="$t('end')"
        value-format="timestamp"
        @change="handleFreshData"
        align="left"
      >
      </el-date-picker>
    </div>

    <div>
      <el-button plain size="small" class="fresh_btn" @click="handleFreshData">
        <i class="el-icon-refresh"></i>
        <span>{{ $t("refresh") }}</span>
      </el-button>
    </div>
  </div>
</template>

<script>
  import { OFFSETUTCTIME } from "@/const";
  export default {
    data() {
      return {
        date: [],

        requestIng: false,
      };
    },

    methods: {
      handleFreshData() {
        if (this.requestIng) return;
        this.requestIng = true;
        let params = {
          current_page: 1,
        };
        if (this.date && this.date.length == 2) {
          params.start_date = this.date[0] - OFFSETUTCTIME;
          params.end_date = this.date[1] - OFFSETUTCTIME;
        }
        this.$store.dispatch("activity/getActivityList", params);
        setTimeout(() => (this.requestIng = false), 1000);
      },
    },
  };
</script>

<style lang="scss" scoped>
  .activity_op {
    display: flex;
    flex-direction: row;
    margin-top: 15px;
    align-items: center;
    justify-content: space-between;
  }

  .fresh_btn {
    margin-left: 25px;
    height: 30px;
  }
</style>
