<template>
  <div class="slow_list_op">
    <div>
      <el-date-picker
        class="datePickerStyle"
        size="small"
        type="datetimerange"
        range-separator="—"
        :start-placeholder="$t('start')"
        :end-placeholder="$t('end')"
        :picker-options="$root.pickerOptions"
        align="left"
        value-format="yyyy-MM-dd HH:mm:ss"
        v-model="datetimerange"
        @change="handleChange"
      >
      </el-date-picker>
    </div>
    <div>
      <el-button plain size="small" class="fresh_btn" @click="handleChange">
        <i class="el-icon-refresh"></i>
        <span>{{ $t("refresh") }}</span>
      </el-button>
    </div>
  </div>
</template>

<script>
  export default {
    data() {
      return {
        datetimerange: [],
      };
    },
    computed: {},
    methods: {
      handleChange() {
        let params = {
          current_page: 1,
        };
        if (this.datetimerange?.length == 2) {
          params.start_time = this.datetimerange[0];
          params.end_time = this.datetimerange[1];
        }
        this.$store.dispatch("slow/getSlowSqlList", params);
      },
    },
  };
</script>

<style lang="scss" scoped>
  .slow_list_op {
    margin-top: 15px;
    display: flex;
    flex-direction: row;
    align-items: center;
    justify-content: space-between;
  }

  .datePickerStyle {
    position: relative;
    top: 1px;
  }

  .pointCursor {
    cursor: pointer;
  }
</style>
