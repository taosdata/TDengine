<template>
  <div class="ticket">
    <section class="ticket-item">
      <h2 class="title">{{ $t("billing.voucher") }}</h2>
      <p>
        <span class="num"> 0<span class="float">.09</span> </span
        ><span class="unit">{{ $t("piece") }}</span
        ><span class="tip">（{{ $t("billing.maturityTip") }}）</span>
      </p>
      <el-button class="el-button-cur" type="text">{{
        $t("billing.useVoucher")
      }}</el-button>
    </section>
    <section class="ticket-item">
      <h2 class="title">
        {{ $t("billing.invoicableAmount")
        }}<el-icon class="el-icon-warning-outline" />
      </h2>
      <p>
        <span class="num"> 0<span class="float">.09</span> </span
        ><span class="unit">{{ $t("yuan") }}</span>
      </p>
      <el-button class="el-button-cur" type="text">{{
        $t("billing.makingInvoice")
      }}</el-button>
    </section>
    <section class="ticket-item">
      <h2 class="title">{{ $t("billing.validContract") }}</h2>
      <p>
        <span class="num"> 0<span class="float">.09</span> </span
        ><span class="unit">{{ $t("share") }}</span>
      </p>
      <el-button class="el-button-cur" type="text">{{
        $t("billing.makingContract")
      }}</el-button>
    </section>
  </div>
</template>

<script>
export default {};
</script>

<style lang="scss" scoped>
.ticket {
  display: flex;
  align-items: center;
  padding: 20px;
  font-size: 14px;
  &-item {
    width: 30%;
    & + .ticket-item {
      border-left: 1px solid #f3f3f3;
      padding-left: 10px;
    }
    .el-button-cur {
      font-weight: bold;
    }
  }

  .title {
    color: #b2b2b2;
    font-size: 14px;
    margin-bottom: 10px;
    & > i[class^="el-icon-"] {
      margin-left: 5px;
    }
  }
  .num {
    font-size: 18px;
    font-weight: bold;
    padding-right: 5px;
    .float {
      font-size: 13px;
    }
  }
  .unit {
    color: #c2c2c2;
  }
  .tip {
    color: $color-danger;
  }
}
</style>
