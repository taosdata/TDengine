<template>
  <div>
    <!-- <p class="primary-tip" v-html="$t('dataIn.inputCSVFile')"></p> -->
    <CsvData />
  </div>
</template>

<script>
// import InputCSVFile from "../components/inputCSVFile.vue";
import CsvData from '../components/csvData.vue'
import { dataInConnector, party } from "@/utils/config";
export default {
  components: { CsvData },
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