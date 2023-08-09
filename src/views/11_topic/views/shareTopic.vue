<template>
  <div class="share-topic">
    <el-tabs type="card" v-model="activiteName">
      <el-tab-pane name="user" :label="$t('topic.shareTopicUser')">
        <Subscription :topicId='currentTopic'></Subscription>
      </el-tab-pane>
    </el-tabs>
    <div class="topic-example-select">
      <label class="topic-title">{{ $t("topic.topic") }}</label>
      <el-select
        class="topic-select-content"
        v-model="currentTopic"
        @change="$emit('change')"
        size="small"
      >
        <el-option
          v-for="item in topicList"
          :key="item"
          :label="item"
          :value="item"
        ></el-option>
      </el-select>
    </div>
  </div>
</template>
<script>
import { sendSQLReq } from "@/api/gateway/console";
import Subscription from "./subscription.vue";
import { Message } from "element-ui";
export default {
  name: "ShareTopic",
  components: {
    Subscription,
  },
  data() {
    return {
      activiteName: "user",
      currentTopic: "", 
      topicList: [],
    };
  },
  created(){
    this.getTopicList()
  },
  methods: {
    async getTopicList() {
      try {
        await sendSQLReq(`show topics;`)
          .then((res) => {
            this.topicList = res.data.map((data) => {
            return data.join('')
            });
            this.currentTopic=this.topicList[0]
          })
          .catch((err) => {
            // err.desc && Message.error(err.desc);
            return Promise.reject(err);
          });
      } catch (error) {
        console.log(error);
        // Message.error(error.desc);
      }
    },
  },
};
</script>
<style lang="scss" scoped>
.share-topic {
  position: relative;
  .topic-example-select {
    position: absolute;
    right: 0px;
    top: -5px;
  }
  .topic-title {
    margin-right: 10px;
  }
}
</style>