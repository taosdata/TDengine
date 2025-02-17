<template>
  <p class="errorText">
    <span>{{ walRentionPeriodTip[0] }}</span>
    <router-link v-t="'common.explorer'" to="/explorer"></router-link>
    <span>{{ walRentionPeriodTip[1] }}</span>
  </p>
</template>

<script lang="ts" setup>
import { t } from 'locales';

const props = defineProps<{
  dbName: string;
}>();
const walRentionPeriodTip = computed(() => t('topic.changeDBWalRentionPeriodTip', [props.dbName]).split(';'));
</script>

<style scoped lang="scss"></style>
