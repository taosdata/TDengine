<template>
  <div class="group relative no-wrap flex items-center cursor-pointer">
    {{ text }}
    <el-button
      :class="{
        'hidden!': !props.alwaysShow
      }"
      class="absolute right-0 text-[14px] border-none group-hover:block! bg-white! rounded-[2px] c-lightblue"
      type="primary"
      link
      :title="t('common.copy')"
      size="small"
      icon="DocumentCopy"
      @click="copy(text)"
    >
      {{ btnText }}
    </el-button>
  </div>
</template>

<script lang="ts" setup>
import { copy } from 'utils';
import { t } from 'locales';

const props = defineProps({
  text: {
    type: String,
    default: ''
  },
  alwaysShow: {
    type: Boolean,
    default: false
  },
  isShowBtnText: {
    type: Boolean,
    default: false
  }
});
const { text, isShowBtnText } = toRefs(props);
const btnText = computed(() => (isShowBtnText ? t('common.copy') : ''));
</script>
