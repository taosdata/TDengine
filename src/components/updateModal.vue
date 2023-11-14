
<template>
  <div class="update-modal">
    <div class="title">
      {{ i18n.t('systemUpdate') }}
    </div>
    <div class="content">
      {{ i18n.t('updateContent')}} <br/>
    </div>
    <div class="actions">
      <el-button size="small" @click="handleAfterLeave">{{ i18n.t('ignore')}}</el-button>
      <el-button size="small" type='primary' @click="refresh">{{ i18n.t('update')}}</el-button>
    </div>
  </div>
</template>
 
<script>
export default {
  methods: {
    handleAfterLeave() {
      this.$destroy(true);
      this.$el.parentNode.removeChild(this.$el);
    },
    refresh() {
      this.handleAfterLeave();
      // window.location.replace(window.location.href); //没有刷新缓存
        location.reload(true);  // 刷新了缓存
    },
  },
};
</script>
 
<style scoped>
.update-modal {
  user-select: none;
  position: fixed;
  right: 10px;
  bottom: 20px;
  max-width: 340px;
  min-width: 250px;
  width: 50%;
  background-color: #fff;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.3);
  border-radius: 5px;
  padding: 10px 15px;
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
.update-modal .title {
  height: 50px;
  line-height: 50px;
  font-size: 16px;
  margin-bottom: 10px;
}
.update-modal .content {
  font-size: 14px;
}
.update-modal .actions {
  display: flex;
  justify-content: flex-end;
  margin-top: 30px;
}
</style>