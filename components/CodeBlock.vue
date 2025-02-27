<template>
  <pre :key="props.code" v-highlight class="pre-code">
    <code :class="'language-'+props.language">{{props.code}}</code>
  </pre>
</template>

<script lang="ts" setup>
const props = withDefaults(
  defineProps<{
    code: string;
    language?: string;
  }>(),
  {
    language: 'sql'
  }
);
</script>

<style scoped lang="scss">
.pre-code {
  padding: 5px;
  text-align: left;
  white-space: break-spaces;
  background-color: #f6f8fa;
}
</style>
