<template>
  <div>
    <p class="primary-tip" v-html="$t('dataIn.inputCSVFile')"></p>
    <InputCSVFile />
  </div>
</template>

<script>
import InputCSVFile from "../components/inputCSVFile.vue";

import { dataInConnector, party } from "@/utils/config";
export default {
  components: { InputCSVFile },
  docsMap: {
    connector: {
      docsList: dataInConnector,
      parentUrl: "/dataIn",
      urlPre: "/docs/dataInConnector/",
    },
    thirdParty: {
      docsList: party,
      parentUrl: "/dataIn",
      urlPre: "/docs/party/",
    },
  },
};
</script>

<style lang="scss" scoped>
</style>