<template>
  <div>
    <p class="primary-tip" v-html="$t('dataOut.connectorTip')"></p>
    <DocsList v-bind="docsMap.outconnector" />
     <!-- <p class="primary-tip" v-html="$t('dataIn.thirdPartyTip')"></p> -->
    <!-- <DocsList v-bind="docsMap.inthirdParty" />
    <p class="primary-tip" v-html="$t('dataOut.toolsTip')"></p>
    <DocsList v-bind="docsMap.outtool" /> -->
    <!-- <p class="primary-tip flexStart">
      <router-link to="/topic">
        <Icon class="icon-topic" name="topic"></Icon>
      </router-link>
      <span v-html="$t('dataOut.subscriptionTitle')"></span>
    </p> -->
  </div>
</template>

<script>
import DocsList from "@/views/3_dataIn/components/docsList.vue";
import { dataOutConnector, dataOutTool,dataInConnector, party } from "@/utils/config";
export default {
  components: { DocsList },
  data() {
    return {
      docsMap: {
        inconnector: {
          docsList: dataInConnector,
          parentUrl: "/dataIn",
          urlPre: "/docs/dataInConnector/",
        },
        inthirdParty: {
          docsList: party,
          parentUrl: "/dataIn",
          urlPre: "/docs/party/",
        },
        outconnector: {
          docsList: dataOutConnector,
          parentUrl: "/dataOut",
          urlPre: "/docs/dataOutConnector/",
        },
        outtool: {
          docsList: dataOutTool,
          parentUrl: "/dataOut",
          urlPre: "/docs/dataOutTool/",
        },
      },
    };
  },
  computed: {
    toolName() {
      return this.$route.path.slice(1).split("/")[1] || "";
    },
  },
};
</script>

<style lang="scss" scoped>
.icon-topic {
  width: 20px;
  height: 20px;
  margin-right: 10px;
}
</style>
