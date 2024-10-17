<template>
  <div>
    <p class="primary-tip" v-html="$t('dataOut.connectorTip')"></p>
    <DocsList v-bind="docsMap.connector" />
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
import { connector, dataOutTool,dataInConnector, party } from "@/utils/config";
export default {
  components: { DocsList },
  data() {
    return {
      docsMap: {
        connector: {
          docsList: connector,
          parentUrl: "/programming",
          urlPre: "/docs/connector/",
        }
      },
    };
  },
  created(){
    console.log(connector,'connector')
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
