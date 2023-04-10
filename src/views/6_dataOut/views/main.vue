<template>
  <div>
    <p class="primary-tip" v-html="$t('dataOut.toolsTip')"></p>
    <DocsList v-bind="docsMap.tool" />
    <p class="primary-tip flexStart">
      <router-link to="/topic">
        <Icon class="icon-topic" name="topic"></Icon>
      </router-link>
      <span v-html="$t('dataOut.subscriptionTitle')"></span>
      <span style="color:#4259ce;cursor:pointer;" id="directTopic" @click="directTopic">&nbsp;{{$t('dataOut.subscriptions')}}</span>
    </p>
  </div>
</template>

<script>
  import DocsList from "@/views/3_dataIn/components/docsList.vue";
  import { dataOutConnector, dataOutTool } from "@/utils/config";
  export default {
    components: { DocsList },
    data() {
      return {
        docsMap: {
          connector: {
            docsList: dataOutConnector,
            parentUrl: "/dataOut",
            urlPre: "/docs/dataOutConnector/",
          },
          tool: {
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
    methods:{
      directTopic(){
        this.$router.push({
          path:'/topic'
        })
      }
    }
  };
</script>

<style lang="scss" scoped>
  .icon-topic {
    width: 20px;
    height: 20px;
    margin-right: 10px;
  }
</style>
