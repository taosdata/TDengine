<template>
  <div class="bill">
    <h2 class="title">{{ $t("billing.monthlyBilling") }}</h2>
    <ul>
      <li>
        <span>2021年10月</span>
        <span>1000.00元</span>
      </li>
      <li>
        <span>2021年10月</span>
        <span>1000.00元</span>
      </li>
    </ul>
    <div class="more-button">
      <el-button style="font-weight:bold;" type="text">{{
        $t("billing.seeMore")
      }}</el-button>
    </div>
    <section class="icon-block">
      <Icon name="menu_trigger" class="expand-icon" />
    </section>
  </div>
</template>

<script>
export default {};
</script>

<style lang="scss" scoped>
$padding: 20px;
.bill {
  position: relative;
  .title {
    padding: $padding $padding 0;
  }
  ul {
    padding: 0 $padding;
    li {
      padding: $padding 0;
      display: flex;
      justify-content: space-between;
      font-size: 14px;
      color: #000;
      align-items: center;
      border-bottom: 1px solid #eee;
    }
  }
  .more-button {
    text-align: center;
    padding: $padding/4;
  }
  $icon: 20px;
  .icon-block {
    position: absolute;
    @extend .flexCenter;
    text-align: center;
    right: 0;
    bottom: 40px;
    padding: 5px;
    background-color: #fff;
    border-radius: 50%;
    border: 1px solid #e6ebf5;
    box-shadow: 0 2px 12px 0 rgb(0 0 0 / 10%);
    cursor: pointer;
  }
  .expand-icon {
    width: $icon;
    height: $icon;
  }
}
</style>
