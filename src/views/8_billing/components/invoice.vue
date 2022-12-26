<template>
  <div class="invoice">
    <el-button size="small" plain>申请开票</el-button>
    <el-table size="mini" :data="invoiceData">
      <el-table-column label="日期"></el-table-column>
      <el-table-column label="金额"></el-table-column>
      <el-table-column label="发票抬头"></el-table-column>
      <el-table-column label="发票号"></el-table-column>
    </el-table>
  </div>
</template>

<script>
export default {
  data() {
    return {
      invoiceData: []
    };
  }
};
</script>

<style></style>
