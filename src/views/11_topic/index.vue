<template>
  <div class="page-wrapper">
    <MainContentHeader :title="$t('topic.pageTitle')"></MainContentHeader>
    <section class="content">
      <el-tabs value="topic">
        <el-tab-pane name="topic" :label="$t('topic.topic')">
          <Topic></Topic>
        </el-tab-pane>
        <el-tab-pane name="consumer" :label="$t('topic.consumer')">
          <Consumer></Consumer>
        </el-tab-pane>
        <!-- <el-tab-pane name="subscription" :label="$t('topic.subscription')">
          <Subscription></Subscription>
        </el-tab-pane> -->
      </el-tabs>
    </section>
  </div>
</template>

<script>
  import Topic from "./views/topic.vue";
  import Consumer from "./views/consumer.vue";
  // import Subscription from "./views/subscription.vue";
  export default {
    components: {
      Topic,
      Consumer,
      // Subscription,
    },
  };
</script>

<style></style>
