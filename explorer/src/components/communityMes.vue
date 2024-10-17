
<template>
  <div class="custom-modal" v-if="showme">
    <div class="title">
      {{ $t('systemPrompt') }}
    </div>
    <div class="custom-content">
      {{ $t('communityContent')}} <br/>
    </div>
    <el-checkbox v-model="checked">{{ $t('dontDisturbMe')}}</el-checkbox>
    <div class="actions">
      <el-button size="small" @click="handleAfterLeave">{{ $t('close')}}</el-button>
      <el-button size="small">
        <a :href="url" target="_blank">{{ $t('contact') }}</a>
      </el-button>
    </div>
  </div>
</template>
 
<script>
export default {
  data() {
    return {
      checked: false,
      showme: false,
    }
  },
  computed: {
    url() {
      return this.$i18n.locale.includes('en') ? "https://tdengine.com/contact/" : "https://www.taosdata.com/contactUs";
    },
  },
  created() {
    let ts = localStorage.getItem('modalLastCheckedTime');
    if (!ts) {
      this.showme = true
      return;
    }

    ts = parseInt(ts);
    let now = new Date().getTime();
    if (now < ts && (now + 7 * 24 * 3600 * 1000) > ts) {
      this.showme = false
    } else {
      this.showme = true
    }
  },
  methods: {
    handleAfterLeave() {
      let now = new Date().getTime();
      if (this.checked) {
        localStorage.setItem('modalLastCheckedTime', now + 7 * 24 * 3600 * 1000);
      } else {
        localStorage.setItem('modalLastCheckedTime', now + 24 * 3600 * 1000);
      }
      this.showme = false
    }
  },
};
</script>
 
<style scoped>
.custom-modal {
  user-select: none;
  position: fixed;
  right: 18px;
  bottom: 20px;
  max-width: 500px;
  min-width: 250px;
  width: 50%;
  background-color: #fafafa;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.3);
  border-radius: 5px;
  padding: 10px 20px;
  animation: shakeY 1.5s linear;
  z-index: 99999;
}
@keyframes shakeY {
  from,
  to {
    transform: translate3d(0, 0, 0);
  }
 
  10%,
  30%,
  50%,
  70%,
  90% {
    transform: translate3d(0, -10px, 0);
  }
 
  20%,
  40%,
  60%,
  80% {
    transform: translate3d(0, 10px, 0);
  }
}
 
.shakeY {
  animation-name: shakeY;
}
.custom-modal .title {
  height: 40px;
  line-height: 40px;
  font-size: 16px;
  margin-bottom: 10px;
}
.custom-modal .custom-content {
  font-size: 14px;
  margin-bottom: 8px;
}
.custom-modal .actions {
  display: flex;
  justify-content: flex-end;
  margin-top: 30px;
}
</style>