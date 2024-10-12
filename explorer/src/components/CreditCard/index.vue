<template>
  <div class="card-form">
    <CreditAddress @save="save" :requestIng="requestIng" @back="back" @cancel="$emit('close')" v-show="isNext" />
    <el-form v-show="!isNext" ref="cardForm" size="small" :model="info" :rules="rules">
      <el-form-item required prop="cardNumber">
        <template v-if="hasCard" slot="label">
          <span
            >{{ $t("billing.cardNumber") }}
            <el-switch
              style="margin-left: 10px; z-index: 5"
              @change="updateCardChange"
              size="mini"
              v-model="ifUpdateCard"
              active-color="#13ce66"
              inactive-color="#ff4949"
            >
            </el-switch
          ></span>
        </template>
        <CardNumber :placeholder="$t('billing.cardNumber')" :disabled="!ifUpdateCard" v-model="info.cardNumber"></CardNumber>
      </el-form-item>
      <el-row>
        <el-col :span="11">
          <el-form-item :label="$t('expiration')" prop="expiration" required>
            <el-date-picker
              :disabled="!ifUpdateCard"
              :picker-options="{ disabledDate }"
              v-model="info.expiration"
              type="month"
              value-format="MM/yyyy"
              placeholder="MM/YYYY"
              format="MM/yyyy"
              :clearable="false"
            ></el-date-picker>
          </el-form-item>
        </el-col>
        <el-col :span="11" :offset="2">
          <el-form-item :label="$t('billing.cvc')" prop="cvcCode" required>
            <el-input
              :disabled="!ifUpdateCard"
              v-model="info.cvcCode"
              :maxlength="6"
              :placeholder="$t('billing.cvc')"
              clearable
              @input="cvcInput"
            ></el-input>
          </el-form-item>
        </el-col>
      </el-row>
      <el-form-item :label="$t('billing.cardHolder')" prop="cardOwnerName" required>
        <el-input v-model="info.cardOwnerName" maxlength="32" :placeholder="$t('billing.cardHolder')"></el-input>
      </el-form-item>
      <!-- <el-form-item :label="$t('companyName')" prop="companyName">
        <el-input v-model="info.companyName" :placeholder="$t('companyName')"></el-input>
      </el-form-item> -->
      <el-row>
        <el-col :span="11">
          <el-form-item label="">
            <el-button @click="$emit('close')" :disabled="requestIng" style="width: 100%" plain>{{ $t("cancel") }}</el-button>
          </el-form-item>
        </el-col>
        <el-col :span="11" :offset="2">
          <el-form-item label="">
            <el-button @click="next" :disabled="requestIng" :loading="requestIng" style="width: 100%" type="primary">{{ $t("continue") }}</el-button>
          </el-form-item>
        </el-col>
      </el-row>
    </el-form>
  </div>
</template>

<script>
  import CardNumber from "./CardNumber.vue";
  import CreditAddress from "./CreditAddress";
  import { OFFSETUTCTIME } from "@/const";
  export default {
    components: { CardNumber, CreditAddress },
    data() {
      return {
        info: {
          cardNumber: "",
        },
        rules: {},
        isNext: false,
        requestIng: false,
        ifUpdateCard: false,
      };
    },
    computed: {
      hasCard() {
        return !!this.$store.state.billing.creditCardInfo.cardNumber;
      },
      cardInfo() {
        return this.$store.state.billing.creditCardInfo;
      },
      disabledDate() {
        return date => {
          return date.getTime() < Date.now() + OFFSETUTCTIME;
        };
      },
    },
    watch: {
      cardInfo: {
        handler(val) {
          if (val.cardNumber) {
            this.info = {
              cardNumber: "#### #### #### " + val.cardNumber.slice(-4),
              expiration: val.cardMonth + "/" + val.cardYear,
              cvcCode: val.cvcCode || "####",
              cardOwnerName: val.cardOwnerName,
              companyName: val.companyName,
            };
            this.ifUpdateCard = false;
          } else {
            this.info = {
              cardNumber: "",
              expiration: "",
              cvcCode: "",
              cardOwnerName: "",
              companyName: "",
            };
            this.ifUpdateCard = true;
          }
          this.isNext = false;
        },
        deep: true,
        immediate: true,
      },
    },
    methods: {
      next() {
        this.$refs.cardForm.validate(valid => {
          if (valid) {
            this.isNext = true;
          }
        });
      },
      cvcInput(val) {
        this.info.cvcCode = val.replace(/\D/g, "");
      },
      save(addressInfo) {
        if (this.requestIng) return;
        this.requestIng = true;
        const expiration = this.info.expiration.split("/");
        const payload = {
          ...addressInfo,
          cardOwnerName: this.info.cardOwnerName,
          cardExpMonth: expiration[0],
          cardExpYear: expiration[1],
          cardNumber: 0,
          cvcCode: 0,
        };
        if (this.ifUpdateCard || !this.hasCard) {
          payload.cardNumber = this.info.cardNumber;
          payload.cvcCode = this.info.cvcCode;
        }
        if (this.hasCard) {
          payload.ifUpdateCard = this.ifUpdateCard ? 1 : 0;
          payload.id = this.$store.state.billing.creditCardInfo.id;
        }
        this.$store
          .dispatch("billing/updatePaymentMethod", payload)
          .then(() => {
            this.$emit("close");
            this.$message.success(this.hasCard ? this.$t("updateSucc") : this.$t("addSucc"));
            this.$store.dispatch("billing/getPaymentMethod");
          })
          .finally(() => {
            this.requestIng = false;
          });
      },
      back() {
        this.isNext = false;
      },
      updateCardChange(val) {
        if (val) {
          this.info = {
            cardNumber: "",
            expiration: this.cardInfo.cardMonth + "/" + this.cardInfo.cardYear,
            cvcCode: "",
            cardOwnerName: this.cardInfo.cardOwnerName,
          };
        } else {
          this.info = {
            cardNumber: "#### #### #### " + this.cardInfo.cardNumber.slice(-4),
            expiration: this.cardInfo.cardMonth + "/" + this.cardInfo.cardYear,
            cvcCode: this.cardInfo.cvcCode || "####",
            cardOwnerName: this.cardInfo.cardOwnerName,
          };
        }
      },
    },
  };
</script>

<style lang="scss" scoped>
  .card-form {
    max-width: 500px;
    margin: auto;
  }
</style>
