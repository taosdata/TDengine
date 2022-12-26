<template>
  <div>
    <DocsList v-bind="visualize" />
  </div>
</template>

<script>
  import DocsList from "@/views/3_dataIn/components/docsList.vue";
  import { visualize } from "@/utils/config";
  export default {
    components: { DocsList },
    data() {
      return {
        visualize: {
          docsList: visualize,
          parentUrl: "/visualize",
          urlPre: "/docs/visualize/",
        },
      };
    },
  };
</script>

<style></style>
