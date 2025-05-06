<template>
  <div v-dompurify-html="html" class="markdown-body"></div>
</template>

<script lang="ts" setup>
import 'github-markdown-css/github-markdown-light.css';
import 'highlight.js/styles/monokai-sublime.css';
import markdownIt from 'utils/markdownIt';

const props = withDefaults(
  defineProps<{
    content: string;
  }>(),
  {
    content: ''
  }
);
const html = computed(() => markdownIt.render(props.content));
</script>

<style scoped></style>
