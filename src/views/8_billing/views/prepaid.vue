<template>
  <div class="prepaid">
    <section class="content-wrapper">
      <el-card shadow="always" class="content-item">
        <PannelHeader title="账单" />
        <Bill />
      </el-card>
      <el-card shadow="always" class="content-item">
        <PannelHeader title="使用明细" />
        <Usage />
      </el-card>
      <el-card shadow="always" class="content-item">
        <PannelHeader title="充值" />
        <Recharge />
      </el-card>
      <el-card shadow="always" class="content-item">
        <PannelHeader title="发票" />
        <Invoice />
      </el-card>
      <el-card shadow="always" class="content-item">
        <PannelHeader title="通知" />
        <Notice />
      </el-card>
    </section>
  </div>
</template>

<script>
import {
  PannelHeader,
  Bill,
  Usage,
  Recharge,
  Invoice,
  Notice
} from "../components";

export default {
  components: { PannelHeader, Bill, Usage, Recharge, Invoice, Notice }
};
</script>

<style lang="scss" scoped>
.content-wrapper {
  width: 100%;
  display: flex;
  flex-wrap: wrap;
  justify-content: space-between;
  .content-item {
    width: 48%;
    margin-bottom: 20px;
  }
}
</style>
