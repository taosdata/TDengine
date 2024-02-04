<template>
  <div class="page-wrapper">
    <MainContentHeader :title="$t('topic.pageTitle')"></MainContentHeader>
    <section class="content">
      <el-tabs value="topic">
        <LinkTab :tabs="isOEM?oemTabs:tabs" class="topic-heads" />
        <router-view></router-view>
        <!-- <el-tab-pane name="topic" :label="$t('topic.topic')">
          <Topic></Topic>
        </el-tab-pane>
        <el-tab-pane name="consumer" :label="$t('topic.consumer')">
          <Consumer></Consumer>
        </el-tab-pane> -->
      </el-tabs>
    </section>
  </div>
</template>

<script>
import Topic from "./views/topic.vue";
import Consumer from "./views/consumer.vue";
export default {
  provide() {
    return {
      tabs: this.tabs,
      parentName:this.name
    };
  },
  components: {
    Topic,
    Consumer,
  },
  data() {
    return {
      name:'Topic',
      isOEM:
        process.env.VUE_APP_CUS_NAME &&
        process.env.VUE_APP_CUS_NAME !== "TDengine",
    };
  },
  computed: {
    tabs() {
      const tabs = [
        {
          label: this.$t("topic.topic"),
          name: "/topic",
        },
        {
          label: this.$t("topic.consumer"),
          name: "/topic/consumer",
        },
        {
          label: this.$t("topic.shareTopic"),
          name: "/topic/share",
        },
        {
          label: this.$t("topic.sampleCode"),
          name: "/topic/example",
        },
      ]
      return tabs
    },
    oemTabs() {
      const oemTabs = [
        {
          label: this.$t("topic.topic"),
          name: "/topic",
        },
        {
          label: this.$t("topic.consumer"),
          name: "/topic/consumer",
        },
        {
          label: this.$t("topic.shareTopic"),
          name: "/topic/share",
        }
      ]
      return oemTabs
    }
  }
  
};
</script>

<style></style>
