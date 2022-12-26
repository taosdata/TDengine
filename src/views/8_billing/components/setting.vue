<template>
  <div class="setting">
    <el-card style="margin-bottom: 20px">
      <section class="setting-content">
        <section class="left">
          <PanelHeader style="margin-bottom: 20px" :title="$t('billing.creditCardInfo')">
            <el-button @click="updateCard" v-if="cardInfo.cardNumber" class="icon-update" icon="el-icon-edit" size="small" plain></el-button>
          </PanelHeader>
          <Card v-if="cardInfo.cardNumber" :cardInfo="cardInfo" />
          <el-button v-else icon="el-icon-plus" @click="addCard" class="big-button w100" plain>{{ $t("billing.addCredit") }}</el-button>
        </section>
        <section class="right">
          <PaymentEmail />
        </section>
      </section>
    </el-card>
  </div>
</template>

<script>
  import PanelHeader from "./pannelHeader.vue";
  import PaymentEmail from "./paymentEmail.vue";
  import Card from "./creditCard.vue";
  export default {
    components: { Card, PanelHeader, PaymentEmail },
    data() {
      return {
        dialog: false,
      };
    },
    computed: {
      cardInfo() {
        return this.$store.state.billing.creditCardInfo;
      },
    },
    methods: {
      addCard() {
        this.$store.state.billing.creditDialog = true;
      },
      updateCard() {
        this.$store.state.billing.creditDialog = true;
      },
    },
  };
</script>

<style lang="scss" scoped>
  .setting-content {
    display: flex;
    align-items: flex-start;
    .left {
      width: 430px;
      flex-shrink: 0;
    }
    .right {
      flex: 1;
      padding-left: 100px;
    }
  }
  .icon-update {
    position: absolute;
    right: 0;
  }
</style>
