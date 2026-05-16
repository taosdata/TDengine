<template>
  <div>
    <p
      v-dompurify-html="$t('dataIn.thirdPartyTip')"
      class="primary-tip" 
    >
    </p>
    <DocsList v-bind="thirdParty" />
  </div>
</template>

<script setup lang="ts">
import DocsList from "@/components/document/list.vue";
import { party } from "@/components/document/index";

const thirdParty = reactive({
  docsList: party,
  parentUrl: "/dataIn",
  urlPre: "/docs/party/",
});
</script>

<style></style>
