<template>
  <div class="language" @click="switchLanguage">{{ locallanguage }}</div>
</template>

<script setup lang="ts">
import { getLocalLang } from '@/utils';
import i18n from '@/lang';
import { setLocale } from 'taos-ui/config';

const locallanguage = computed(() => {
  if (getLocalLang() == 'zh') {
    return 'EN';
  } else {
    return '中';
  }
});

function switchLanguage() {
  if (getLocalLang() == 'zh') {
    (i18n.global.locale as WritableComputedRef<string>).value = 'en';
    localStorage.setItem('local_language', 'en');
    setLocale('en');
  } else {
    (i18n.global.locale as WritableComputedRef<string>).value = 'zh';
    localStorage.setItem('local_language', 'zh');
    setLocale('zh');
  }
}
</script>

<style scoped>
.language {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 26px;
  height: 26px;
  margin-top: 4px;
  margin-right: 20px;
  color: #4259ce;
  cursor: pointer;
  border: 1px solid #4259ce;
  border-radius: 50%;
}
</style>
