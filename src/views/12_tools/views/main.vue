<template>
  <div>
    <DocsList v-bind="tool" />
  </div>
</template>

<script>
  import DocsList from "@/views/3_dataIn/components/docsList.vue";
  import { tool } from "@/utils/config";
  export default {
    components: { DocsList },
    data() {
      return {
        tool: {
          docsList: tool,
          parentUrl: "/tools",
          urlPre: "/docs/tool/",
        },
      };
    },
  };
</script>

<style lang="scss" scoped></style>
