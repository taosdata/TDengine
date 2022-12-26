<template>
  <div class="search_input_row">
    <div>
      <el-date-picker
        v-model="date"
        size="small"
        type="datetimerange"
        value-format="timestamp"
        :picker-options="$root.pickerOptions"
        range-separator="-"
        :start-placeholder="$t('start')"
        :end-placeholder="$t('end')"
        align="left"
        @change="dateChange"
      >
      </el-date-picker>
    </div>

    <div>
      <el-button plain size="small" @click="handleFreshData">
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
      };
    },

    methods: {
      handleFreshData() {
        this.date = [];
        this.$store.dispatch("alert/getAlertList");
      },
      dateChange() {
        if (!this.date || this.date?.length < 1) {
          return this.$store.dispatch("alert/getAlertList");
        }
        this.$store.dispatch("alert/getAlertList", {
          start: this.date[0] - OFFSETUTCTIME,
          end: this.date[1] - OFFSETUTCTIME,
        });
      },
    },
  };
</script>

<style lang="scss" scoped>
  .search_input_row {
    display: flex;
    flex-direction: row;
    margin-top: 15px;
    align-items: center;
    justify-content: space-between;
  }
</style>
