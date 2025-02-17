<template>
  <SqlEditor v-model="value"></SqlEditor>
</template>

<script lang="ts" setup>
import SqlEditor from 'components/SqlCodeEditor/index.vue';

const value = ref('');
</script>

<style scoped lang="scss"></style>
