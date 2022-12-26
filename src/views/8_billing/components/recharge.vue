<template>
  <div class="recharge">
    <SubTitle title="支付网关" />
    <SubTitle title="支付历史" />
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
          v-model="rechargeDate"
          @change="handleChange"
        >
        </el-date-picker>
      </el-form-item>
      <el-form-item :label="$t('type')">
        <el-radio-group v-model="rechargeType" size="mini">
          <el-radio-button
            v-for="item in typeGroup"
            :key="item.value"
            :label="item.value"
            >{{ item.label }}</el-radio-button
          >
        </el-radio-group>
      </el-form-item>
    </el-form>
    <el-table size="mini" :data="rechargeData">
      <el-table-column label="日期"></el-table-column>
      <el-table-column label="金额"></el-table-column>
      <el-table-column label="余额"></el-table-column>
    </el-table>
  </div>
</template>

<script>
import SubTitle from "./subTitle.vue";
export default {
  components: {
    SubTitle
  }
};
</script>

<style></style>
