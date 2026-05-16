<template>
  <div>
    <DocsList v-bind="toolData" />
  </div>
</template>

<script setup lang="ts">
import DocsList from "@/components/document/list.vue";
import { tool } from "@/components/document/index";
const toolData = reactive({
  docsList: tool,
  parentUrl: "/tools",
  urlPre: "/docs/tool/",
})

</script>

<style lang="scss" scoped></style>
