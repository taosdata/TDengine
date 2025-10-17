<template>
  <div class="share-topic">
    <el-tabs v-model="activityName" type="card">
      <el-tab-pane name="user" :label="$t('topic.shareTopicUser')">
        <Subscription :topic-id="currentTopic"></Subscription>
      </el-tab-pane>
    </el-tabs>
    <div class="topic-example-select">
      <label class="topic-title">{{ $t('topic.topic') }}</label>
      <el-select v-model="currentTopic" class="topic-select-content" size="default" @change="$emit('change')">
        <el-option v-for="item in topicList" :key="item" :label="item" :value="item"></el-option>
      </el-select>
    </div>
  </div>
</template>
<script setup lang="ts">
import { sendSQLReq } from '@/api/explorer';
import Subscription from './subscription.vue';

defineEmits(['change']);

const activityName = ref<string>('user');
const currentTopic = ref<string>('');
const topicList = ref([]);

async function getTopicList() {
  try {
    await sendSQLReq(`show topics;`)
      .then(res => {
        topicList.value = res.data.map(data => {
          return data.join('');
        });
        currentTopic.value = topicList.value[0];
      })
      .catch(err => {
        // err.desc && this.$error(err.desc);
        return Promise.reject(err);
      });
  } catch (error) {
    console.log(error);
    // this.$error(error.desc);
  }
}
getTopicList();
</script>
<style lang="scss" scoped>
.share-topic {
  position: relative;

  .topic-example-select {
    position: absolute;
    top: -5px;
    right: 0;
  }

  .topic-title {
    margin-right: 10px;
  }
}

.topic-select-content {
  width: 200px;
}
</style>
