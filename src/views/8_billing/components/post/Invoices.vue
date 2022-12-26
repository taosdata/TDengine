<template>
  <div class="invoice">
    <el-row class="invoice-card" type="flex">
      <el-col :span="11">
        <el-card class="custom-card">
          <h1 class="title">{{ $t("billing.accountDue") }}</h1>
          <p class="num">$00.00</p>
          <p>due date: 06/10/2022</p>
          <p class="pay-tip"><el-icon class="el-icon-success"></el-icon> {{ $t("billing.noPayment") }}</p>
        </el-card>
      </el-col>
      <el-col :span="11" :offset="1">
        <el-card class="custom-card">
          <h1 class="title">{{ $t("billing.upcomingInvo") }}</h1>
          <p>{{ $t("usageEstimation") }}:</p>
          <p>{{ estimatedUsage }}:</p>
        </el-card>
      </el-col>
    </el-row>
    <el-table :data="[]">
      <el-table-column :label="$t('billing.invoicesID')"></el-table-column>
      <el-table-column :label="$t('date')">
        <template slot-scope="scope">
          <a href="">{{ scope.row.id }}</a>
        </template>
      </el-table-column>
      <el-table-column :label="$t('billing.billingPeriod')">
        <template slot-scope="scope">
          <a href="">{{ scope.row.id }}</a>
        </template>
      </el-table-column>
      <el-table-column :label="$t('billing.invoicesDate')">
        <template slot-scope="scope">
          <a href="">{{ scope.row.id }}</a>
        </template>
      </el-table-column>
      <el-table-column :label="$t('billing.accountDue')">
        <template slot-scope="scope">
          <a href="">{{ scope.row.id }}</a>
        </template>
      </el-table-column>
      <el-table-column :label="$t('status')">
        <template slot-scope="scope">
          <a href="">{{ scope.row.id }}</a>
        </template>
      </el-table-column>
    </el-table>
  </div>
</template>

<script>
export default {
  computed: {
    estimatedUsage() {
      return this.$t("billing.estimatedUsage").replace("{{month}}", this.$t("billing.monthMap")["1"]);
    },
  },
};
</script>

<style lang="scss" scoped>
.num {
  font-size: 20px;
  font-weight: bold;
  line-height: 30px;
}
.invoice-card {
  margin: 20px 0;
}
.custom-card {
  // margin-top: 20px;
  height: 100%;
}
.title {
  font-size: 18px;
  margin-bottom: 10px;
}
.pay-tip {
  display: flex;
  align-items: center;
  & > [class^="el-icon-"] {
    margin-right: 5px;
    color: $color-success;
  }
}
</style>
