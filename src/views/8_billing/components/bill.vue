<template>
  <div class="bill">
    <p class="balance">
      余额 (元) : <span class="nums">0</span>,<span
        >预计还能使用<span class="nums">0</span>天</span
      >
    </p>
    <el-form :inline="true">
      <el-form-item label="时间">
        <el-date-picker
          size="mini"
          type="datetimerange"
          range-separator="—"
          :start-placeholder="$t('start')"
          :end-placeholder="$t('end')"
          :picker-options="$root.pickerOptions"
          align="left"
          value-format="timestamp"
          v-model="datetimerange"
          @change="handleChange"
        >
        </el-date-picker>
      </el-form-item>
      <el-form-item :label="$t('type')">
        <el-radio-group v-model="billType" size="mini">
          <el-radio-button
            v-for="item in typeGroup"
            :key="item.value"
            :label="item.value"
            >{{ item.label }}</el-radio-button
          >
        </el-radio-group>
      </el-form-item>
    </el-form>
    <el-table size="mini" :data="billData">
      <el-table-column label="日期"></el-table-column>
      <el-table-column label="交易类型"></el-table-column>
      <el-table-column label="金额"></el-table-column>
      <el-table-column label="余额"></el-table-column>
    </el-table>
  </div>
</template>

<script>
export default {
  data() {
    return {
      billType: "all",
      billData: []
    };
  },
  computed: {
    typeGroup() {
      return [
        {
          value: "all",
          label: "全部"
        },
        {
          value: "0",
          label: "费用"
        },
        {
          value: "1",
          label: "充值"
        },
        {
          value: "2",
          label: "赠券"
        }
      ];
    }
  }
};
</script>

<style lang="scss" scoped>
.bill {
  padding: 20px;
  background: #fff;
  border-radius: 4px;
  .balance {
    font-size: 16px;
    color: #333;
    margin-bottom: 20px;
    .nums {
      font-size: 18px;
      font-weight: bold;
      padding: 0 5px;
    }
  }
}
</style>
