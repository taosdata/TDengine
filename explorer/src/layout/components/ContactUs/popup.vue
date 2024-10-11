<template>
  <div class="popup">
    <div class="popup-container">
      <div :class="isShowCss">
        <div class="popup-title">
          <div class="popup-title-text">
            {{ $t("footerComp.contactUs") }}
          </div>
          <div class="close-popup" @click="closeBtn">
            <img src="../../../assets/close.webp" alt=" Database" />
          </div>
        </div>
        <div class="popup-content">
          <div style="position: relative">
            <div :class="isShowMsgCss">
              {{ message }}
            </div>
            <span style="margin-bottom: 0.5rem; display: block; color: #6a85bd; font-size: 16px">{{ $t("footerComp.contactUsTitle") }}</span>
            <concatUsFormVue @close="closeBtn" />
          </div>
        </div>
      </div>
    </div>
  </div>
</template>
<script>
  import concatUsFormVue from "./contactUsForm.vue";
  export default {
    props: {
      form: {},
    },
    components: { concatUsFormVue },
    data() {
      this.category = [
        "Cloud service partner",
        "Enterprise Edition Consulting",
        "Channel partner",
        "Integration & Technology Partners",
        "OEM partner",
      ];
      return {
        showMessage: false,
        sucessMsg: "",
        message: "",
        isShow: true,
      };
    },
    computed: {
      isShowCss() {
        return this.isShow ? "display-is-block" : "display-is-none";
      },
      isShowClose() {
        return this.isShow ? "display-is-none" : "display-is-block";
      },
      isShowMsgCss() {
        return this.showMessage ? "popalert" : "popalert popalert-hidden";
      },
    },
    mounted() {},
    watch: {},
    methods: {
      closeBtn() {
        this.$emit("input", false);
      },
    },
  };
</script>
<style scoped>
  .popup {
    /* 定位 */
    position: fixed;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    /* 显示 */
    z-index: 1000;
    background-color: rgba(0, 0, 0, 0.6);
    transition: z-index 0.4s, opacity 0.4s;
  }

  .popup-hidden {
    display: none;
    margin-top: -1000%;
    opacity: 0;
    z-index: -101;
  }

  .popup-container {
    position: absolute;
    top: 15%;
    left: 50%;
    transform: translateX(-50%);
  }

  .popup-title {
    font-weight: 500;
    font-size: 24px;
    width: 100%;
    display: block;
    border-radius: 0.25rem 0.25rem 0 0;
    position: relative;
    height: 90px;
    background-image: url("../../../assets/banner-bg.webp");
    background-repeat: no-repeat;
    background-position: center center;
    background-size: cover;
  }

  .popup-title-text {
    display: inline-block;
    height: 90px;
    line-height: 90px;
    width: 100%;
    font-weight: 600;
    text-align: center;
    color: #ffffff;
  }

  .close-popup {
    width: 20px;
    position: absolute;
    right: 1rem;
    z-index: 1;
    cursor: pointer;
    top: 0;
  }

  .close-popup img {
    width: 20px;
  }

  .popup-content {
    position: relative;
    background-color: white;
    border-radius: 0.2rem;
    padding: 1rem;
    color: rgba(0, 0, 0, 0.6);
  }

  .popup-content img {
    width: 100%;
  }

  .contract-sales {
    font-size: 16px;
    outline: 0;
    color: rgb(51, 56, 68);
    text-indent: 0.4em;
    width: 100%;
    height: 30px;
    border: 1px solid rgb(0, 118, 206);
    -webkit-border-radius: 4px;
    border-radius: 4px;
    -webkit-transition: border-left 0.2s;
    -o-transition: border-left 0.2s;
    transition: border-left 0.2s;
    vertical-align: top;
    font-weight: 400;
    margin-bottom: 10px;
    box-sizing: border-box;
  }

  .contract-sales:focus {
    border-left: 15px solid rgb(0, 118, 206);
  }

  .btn-primary {
    background-color: rgb(0, 118, 206);
    color: #ffffff;
    -webkit-box-shadow: 0 0 0 0 rgb(255 255 255 / 55%);
    box-shadow: 0 0 0 0 rgb(255 255 255 / 55%);
    -webkit-transition: all 0.2s;
    -o-transition: all 0.2s;
    transition: all 0.2s;
    padding: 8px 15px;
    display: block;
    margin: 0 auto;
    border: 0px solid rgb(0, 118, 206);
    font-size: 16px;
    /* height: 30px; */
    /* line-height: 50px; */
  }

  .btn-primary:hover {
    background-color: rgb(0, 118, 206);
    -webkit-box-shadow: 4px 4px 0 0 var(--b1t);
    box-shadow: 4px 4px 0 0 rgba(0, 118, 206, 0.15);
    -webkit-transform: translate(-2px, -2px);
    -ms-transform: translate(-2px, -2px);
    transform: translate(-2px, -2px);
    cursor: pointer;
  }

  textarea.message {
    font-size: 16px;
    outline: 0;
    color: rgb(51, 56, 68);
    /* padding-left: 0.4em; */
    width: 100%;
    border: solid 1px rgb(0, 118, 206);
    display: inline-block;
    -webkit-border-radius: 4px;
    border-radius: 4px;
    -webkit-transition: border-left 0.2s;
    -o-transition: border-left 0.2s;
    transition: border-left 0.2s;
    vertical-align: top;
    font-weight: 400;
    margin-bottom: 0.5rem;
    padding: 10px 0px 10px 0.4em;
    font-family: Lato, sans-serif;
    line-height: 1.5;
    box-sizing: border-box;
  }

  .popalert {
    font-size: 20px;
    display: block;
    position: absolute;
    /* top: 50%; */
    left: 50%;
    -webkit-transform: translate(-50%, -100%);
    -ms-transform: translate(-50%, -100%);
    transform: translate(-50%, -100%);
    background-color: rgba(0, 0, 0, 0.1);
    color: red;
    padding: 5px 10px;
    border-radius: 10px;
  }

  .popalert-hidden {
    display: none !important;
  }

  .display-is-block {
    display: block;
    width: 400px;
    background-color: #ffffff;
    border-radius: 0.25rem;
  }

  .display-is-none {
    display: none;
    width: 400px;
    background-color: #ffffff;
  }

  .success-img {
    width: 30%;
    margin: 0 auto;
  }

  .success-msg {
    font-size: 20px;
    color: #b3b4b9;
    margin-top: 10px;
    display: block;
    text-align: center;
    margin-bottom: 10px;
  }
</style>
