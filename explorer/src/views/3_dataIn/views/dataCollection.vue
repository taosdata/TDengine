<template>
  <div>
    <!-- <p class="primary-tip" v-html="$t('dataIn.connectorTip')"></p>
    <DocsList v-bind="docsMap.connector" /> -->
    <p class="primary-tip" v-html="$t('dataIn.thirdPartyTip')"></p>
    <DocsList v-bind="docsMap.thirdParty" />
    
  </div>
</template>

<script>
  import DocsList from "../components/docsList";
  import { dataInConnector, party } from "@/utils/config";
  export default {
    components: { DocsList },
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
