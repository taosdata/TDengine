<template>
  <VuePaycard class="card-item" :labels="labels" :value-fields="valueFields" :set-type="setType" />
  <!-- <el-card style="width: 500px; margin-top: 10px">
    <div data-cy="card-info" class="sc-hzbjYJ grQrqf">
      <img alt="unionpay logo" class="sc-HEwFl sc-gWaPwi eyRxth fmQIdv" />
      <div class="sc-LEJYB bExuYm">unionpay{{cardInfo.cardNumber | handleCardNumber}}</div>
      <div class="sc-kOyqGX euzLVp">Exp 04/30</div>
      <div class="sc-iiCSmI ghxMeM">{{cardInfo.cardName}}</div>
      <div v-show="false" class="sc-iiCSmI ghxMeM"><span data-cy="set-primary-button" class="sc-gCEpsI blkfis">Set as primary</span></div>
      <div class="sc-hatQeL fwIgKe">
        <div title="Edit payment method" class="css-0">
          <svg width="16" height="16" viewBox="0 0 16 16" fill="#5B6272" xmlns="http://www.w3.org/2000/svg">
            <path
              fill-rule="evenodd"
              clip-rule="evenodd"
              d="M13.09 6.64897L13.0809 6.6579L5.86216 13.5485C5.71897 13.6852 5.54573 13.7864 5.35636 13.844L1.67461 14.9646C1.46263 15.0291 1.23234 14.9715 1.07565 14.8148C0.918968 14.6581 0.861396 14.4278 0.925913 14.2159L2.04165 10.5499C2.10509 10.3414 2.22122 10.1529 2.37883 10.0024L9.58058 3.12802L11.4376 1.27102C11.9322 0.776358 12.7343 0.776359 13.2289 1.27102L14.9528 2.99488C15.4474 3.48955 15.4474 4.29155 14.9528 4.78622L13.09 6.64897ZM2.40148 13.489L5.00696 12.696C5.01693 12.693 5.02605 12.6877 5.03359 12.6805L11.8081 6.2139L9.99016 4.39599L3.2074 10.8704C3.19911 10.8784 3.193 10.8883 3.18966 10.8993L2.40148 13.489ZM10.8484 3.55722L12.2861 2.11955C12.3121 2.09352 12.3544 2.09352 12.3804 2.11955L14.1042 3.84341C14.1303 3.86944 14.1303 3.91165 14.1042 3.93769L12.6666 5.37535L10.8484 3.55722Z"
            ></path>
          </svg>
        </div>
        <div class="" data-tooltipped="" aria-describedby="tippy-tooltip-8" style="display: inline">
          <div title="Delete payment method" class="css-1hcehcy">
            <svg width="16" height="16" viewBox="0 0 16 16" fill="#C0C4CC" xmlns="http://www.w3.org/2000/svg">
              <path
                fill-rule="evenodd"
                clip-rule="evenodd"
                d="M5.71052 0.798421C6.10999 0.416206 6.6536 0.200012 7.22217 0.200012H8.77773C9.34631 0.200012 9.88991 0.416206 10.2894 0.798421C10.6885 1.18035 10.9111 1.69642 10.9111 2.23257V3.17676H14.2222C14.3782 3.17676 14.5261 3.23614 14.6339 3.33923C14.7413 3.44204 14.8 3.57955 14.8 3.72094C14.8 3.86234 14.7413 3.99984 14.6339 4.10265C14.5261 4.20574 14.3782 4.26513 14.2222 4.26513H13.2444V13.7675C13.2444 14.3036 13.0219 14.8197 12.6227 15.2016C12.2232 15.5838 11.6796 15.8 11.1111 15.8H4.88884C4.32026 15.8 3.77666 15.5838 3.37719 15.2016C2.97802 14.8197 2.75551 14.3036 2.75551 13.7675V4.26513H1.77773C1.62171 4.26513 1.47376 4.20574 1.36602 4.10265C1.25857 3.99984 1.19995 3.86234 1.19995 3.72094C1.19995 3.57955 1.25857 3.44204 1.36602 3.33923C1.47376 3.23614 1.62171 3.17676 1.77773 3.17676H5.08884V2.23257C5.08884 1.69642 5.31135 1.18035 5.71052 0.798421ZM7.22217 1.28838C6.96563 1.28838 6.71791 1.38581 6.53393 1.56184C6.34965 1.73817 6.2444 1.97924 6.2444 2.23257V3.17676H9.75551V2.23257C9.75551 1.97924 9.65025 1.73817 9.46597 1.56184C9.28199 1.38581 9.03428 1.28838 8.77773 1.28838H7.22217ZM12.0888 4.26513H3.91106V13.7675C3.91106 14.0208 4.01632 14.2619 4.2006 14.4382C4.38458 14.6142 4.63229 14.7116 4.88884 14.7116H11.1111C11.3676 14.7116 11.6153 14.6142 11.7993 14.4382C11.9836 14.2619 12.0888 14.0208 12.0888 13.7675V4.26513Z"
              ></path>
              <path
                d="M6.21655 7.33336C6.21655 7.08483 6.41802 6.88336 6.66655 6.88336C6.91508 6.88336 7.11655 7.08483 7.11655 7.33336V11.3334C7.11655 11.5819 6.91508 11.7834 6.66655 11.7834C6.41802 11.7834 6.21655 11.5819 6.21655 11.3334V7.33336Z"
              ></path>
              <path
                d="M8.88322 7.33336C8.88322 7.08483 9.0847 6.88336 9.33323 6.88336C9.58175 6.88336 9.78323 7.08483 9.78323 7.33336V11.3334C9.78323 11.5819 9.58175 11.7834 9.33323 11.7834C9.0847 11.7834 8.88322 11.5819 8.88322 11.3334V7.33336Z"
              ></path>
            </svg>
          </div>
        </div>
      </div>
    </div>
  </el-card> -->
</template>

<script>
  import VuePaycard from "vue-paycard";
  export default {
    props: {
      cardInfo: {
        type: Object,
        default: () => ({}),
      },
    },
    components: { VuePaycard },
    data() {
      return {
        dialog: false,
      };
    },
    filters: {
      handleCardNumber(value) {
        if (!value) return "";
        return value.replace(/\s/g, "").replace(/(\d{4})/g, "$1 ");
      },
    },
    computed: {
      valueFields() {
        return this.$store.state.billing.creditCardInfo || { cardName: "", cardNumber: "", cardMonth: "", cardYear: "", cardCvv: "" };
      },
      setType() {
        return this.$store.state.billing.creditCardInfo.cardBrand || "";
      },
      labels() {
        return { cardName: "Full Name", cardHolder: "Card Holder", cardMonth: "MM", cardYear: "YY", cardExpires: "Expires", cardCvv: "CVV" };
      },
    },
  };
</script>

<style lang="scss" scoped>
  .title {
    font-size: 14px;
    font-weight: bold;
  }
  .tip {
    font-size: 12px;
  }
  .card-item {
    width: 430px;
    margin: 0 0;
    box-shadow: none;
    &::v-deep .card-item__side {
      box-shadow: none;
    }
  }
  .grQrqf {
    width: 100%;
    display: grid;
    row-gap: 8px;
    grid-template:
      "number brand" 1fr
      "expiration ." 1fr
      "primary buttons" 1fr / 1fr 1fr;
    -webkit-box-align: center;
    align-items: center;
  }
  .fmQIdv {
    grid-area: brand / brand / brand / brand;
    justify-self: end;
  }
  .eyRxth {
    max-width: 43px;
    max-height: 20px;
    object-fit: contain;
  }
  .bExuYm {
    grid-area: number / number / number / number;
    display: flex;
    -webkit-box-align: center;
    align-items: center;
    font-weight: 700;
    font-size: 14px;
    line-height: 20px;
    letter-spacing: -0.01em;
    color: rgb(20, 30, 53);
    text-transform: capitalize;
  }
  .euzLVp {
    grid-area: expiration / expiration / expiration / expiration;
    font-size: 14px;
  }
  .ghxMeM {
    grid-area: primary / primary / primary / primary;
    color: $color-primary;
    font-size: 12px;
    line-height: 18px;
    cursor: pointer;
  }
  .fwIgKe {
    display: flex;
    grid-area: buttons / buttons / buttons / buttons;
    justify-self: end;
    gap: 19px;
  }
  .css-1hcehcy {
    align-self: start;
  }
  .blkfis {
    color: rgb(91, 98, 114);
    cursor: pointer;
  }
</style>
