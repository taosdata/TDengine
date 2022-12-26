<template>
  <section class="blance">
    <h2 class="first">{{ $t("billing.availableBalance") }}</h2>
    <div class="second">
      <div>
        <span class="int">0</span>
        <span class="float">.0{{ $t("yuan") }}</span>
      </div>
      <el-button type="primary" size="small">{{
        $t("billing.recharge")
      }}</el-button>
      <el-button size="small">{{ $t("billing.viewDeductions") }}</el-button>
      <el-button size="small" type="text">{{
        $t("billing.withdraw")
      }}</el-button>
    </div>
    <section class="third">
      <section>
        <p class="title">{{ $t("billing.cashAmount") }}</p>
        <p class="num">0.00{{ $t("yuan") }}</p>
      </section>
      <section class="symbol">+</section>
      <section>
        <p class="title">
          {{ $t("billing.credit") }}<el-icon class="el-icon-warning-outline" />
        </p>
        <p class="num">0.00{{ $t("yuan") }}</p>
      </section>
      <section class="symbol">-</section>
      <section>
        <p class="title">{{ $t("billing.amountInArrears") }}</p>
        <p class="num">0.00{{ $t("yuan") }}</p>
      </section>
      <section class="symbol">-</section>
      <section>
        <p class="title">
          {{ $t("billing.frozenAmount")
          }}<el-icon class="el-icon-warning-outline" />
        </p>
        <p class="num">0.00{{ $t("yuan") }}</p>
      </section>
    </section>
  </section>
</template>

<script>
export default {};
</script>

<style lang="scss" scoped>
$padding: 20px;

.first {
  padding: $padding $padding 0;
}
.second {
  display: flex;
  padding: 0 $padding;
  align-items: center;
  margin: 10px 0;
  & > div:first-child {
    margin-right: 10px;
    font-weight: bold;
    font-size: 16px;
    .int {
      font-size: 20px;
    }
  }
}
.third {
  background-color: #fafafa;
  padding: 10px $padding;
  display: flex;
  align-items: center;
  .title {
    font-size: 14px;
    color: #b2b2b2;
    margin-bottom: 5px;
    font-weight: bold;
    & > i[class^="el-icon-"] {
      margin-left: 5px;
    }
  }
  .num {
    font-weight: bold;
    color: #000;
    font-size: 16px;
  }
  .symbol {
    padding: 0 40px;
    color: #000;
    // font-weight: bold;
    font-size: 20px;
  }
}
</style>
