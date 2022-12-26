<template>
  <div>
    <p class="primary-tip" v-html="$t('dataIn.connectorTip')"></p>
    <DocsList v-bind="docsMap.connector" />
    <p class="primary-tip" v-html="$t('dataIn.thirdPartyTip')"></p>
    <DocsList v-bind="docsMap.thirdParty" />
    <p class="primary-tip" v-html="$t('dataIn.inputCSVFile')"></p>
    <InputCSVFile />
  </div>
</template>

<script>
  import DocsList from "../components/docsList";
  import InputCSVFile from "../components/inputCSVFile.vue";
  import { dataInConnector, party } from "@/utils/config";
  export default {
    components: { DocsList, InputCSVFile },
    data() {
      return {
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
    },
  };
</script>

<style></style>
