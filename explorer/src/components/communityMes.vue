<template>
  <div v-if="showme" class="custom-modal">
    <div class="title">
      {{ $t('systemPrompt') }}
    </div>
    <div class="custom-content">{{ $t('communityContent') }} <br /></div>
    <el-checkbox v-model="checked">{{ $t('dontDisturbMe') }}</el-checkbox>
    <div class="actions">
      <el-button size="small" @click="handleAfterLeave">{{ $t('close') }}</el-button>
      <el-button size="small">
        <a :href="url" target="_blank">{{ $t('contact') }}</a>
      </el-button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { getLocalLang } from '@/utils';

const checked = ref<boolean>(false);
const showme = ref<boolean>(false);

const url = computed(() => {
  return getLocalLang().includes('en') ? 'https://tdengine.com/contact/' : 'https://www.taosdata.com/contactUs';
});

function init() {
  let ts: number | string = localStorage.getItem('modalLastCheckedTime') ?? '';
  if (!ts) {
    showme.value = true;
    return;
  }

  ts = parseInt(ts);
  const now: number = new Date().getTime();
  if (now < ts && now + 7 * 24 * 3600 * 1000 > ts) {
    showme.value = false;
  } else {
    showme.value = true;
  }
}
function handleAfterLeave() {
  const now: number = new Date().getTime();
  if (checked.value) {
    localStorage.setItem('modalLastCheckedTime', String(now + 7 * 24 * 3600 * 1000));
  } else {
    localStorage.setItem('modalLastCheckedTime', String(now + 24 * 3600 * 1000));
  }
  showme.value = false;
}
init();
</script>

<style scoped>
.custom-modal {
  position: fixed;
  right: 18px;
  bottom: 20px;
  z-index: 99999;
  width: 50%;
  min-width: 250px;
  max-width: 500px;
  padding: 10px 20px;
  user-select: none;
  background-color: #fafafa;
  border-radius: 5px;
  box-shadow: 0 1px 3px rgb(0 0 0 / 30%);
  animation: shake-y 1.5s linear;
}

@keyframes shake-y {
  0%,
  100% {
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

.shake-y {
  animation-name: shake-y;
}

.custom-modal .title {
  height: 40px;
  margin-bottom: 10px;
  font-size: 16px;
  line-height: 40px;
}

.custom-modal .custom-content {
  margin-bottom: 8px;
  font-size: 14px;
}

.custom-modal .actions {
  display: flex;
  justify-content: flex-end;
  margin-top: 30px;
}
</style>
