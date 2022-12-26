<template>
  <div class="post-paid">
    <el-tabs v-model="$store.state.billing.activeTab">
      <el-tab-pane name="payment" :label="$t('billing.payment')">
        <Payment />
      </el-tab-pane>
      <el-tab-pane name="usage" :label="$t('dashboard.usage')">
        <Usage />
      </el-tab-pane>
      <el-tab-pane name="setting" :label="$t('billing.paymentSetting')">
        <Setting />
      </el-tab-pane>
    </el-tabs>
    <el-dialog align="center" width="520px" :title="$t('billing.creditCardInfo')" :visible.sync="dialog">
      <CreditCardDetail @close="close" />
    </el-dialog>
  </div>
</template>

<script>
  import { Usage, Payment, Setting } from "../components";
  import CreditCardDetail from "@/components/CreditCard";
  export default {
    components: {
      Setting,
      Usage,
      Payment,
      CreditCardDetail,
    },
    data() {
      return {};
    },
    computed: {
      dialog: {
        get() {
          return this.$store.state.billing.creditDialog;
        },
        set(val) {
          this.$store.state.billing.creditDialog = val;
        },
      },
    },
    created() {
      this.$store.dispatch("billing/getBillingOverview");
    },
    methods: {
      handleTabChange(tab) {
        this.activeTab = tab.name;
      },
      close() {
        this.$store.state.billing.creditDialog = false;
      },
    },
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
