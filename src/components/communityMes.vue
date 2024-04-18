
<template>
  <div class="custom-modal">
    <div class="title">
      {{ $t('systemPrompt') }}
    </div>
    <div class="custom-content">
      {{ $t('communityContent')}} <br/>
    </div>
    <el-checkbox v-model="checked">7天内不再提醒</el-checkbox>
    <div class="actions">
      <el-button size="small" @click="handleAfterLeave">{{ $t('close')}}</el-button>
      <el-button size="small" type='primary' @click="refresh">{{ $t('contact')}}</el-button>
    </div>
  </div>
</template>
 
<script>
export default {
  data() {
    return {
      checked: false
    }
  },
  watch: {
    "$store.state.app.showSystemMes": {
      handler(val) {
        console.log('000-------');
        if (val) {
          clearTimeout(this.timer)
        }
      }
    }
  },
  methods: {
    handleAfterLeave() {
      this.$store.commit('app/SET_SHOW_SYSTEM_MES',false)
      if (this.checked) {
        this.timer = setTimeout(() => {
          console.log('777777');
          this.$store.commit('app/SET_SHOW_SYSTEM_MES',true)
        }, 604800000);
      } else {
        localStorage.setItem('modalLastCheckedTime',86400000)
        this.timer = setTimeout(() => {
          console.log('888888');
        }, 86400000);
      }
    },
    refresh() {
      this.handleAfterLeave();
        location.reload(true);  // 刷新了缓存
    },
  },
};
</script>
 
<style scoped>
.custom-modal {
  user-select: none;
  position: fixed;
  right: 18px;
  bottom: 20px;
  max-width: 340px;
  min-width: 250px;
  width: 50%;
  background-color: #fff;
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