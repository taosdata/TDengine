<template>
  <Create v-if="isCreate" :topic-list="topicList" @close="close" />
  <List v-else :topic-list="topicList" @update="getData" @add="isCreate = true" />
</template>

<script lang="ts" setup>
import { getTopicList } from '../api';
import List from './list.vue';
import Create from './create.vue';

const topicList = ref<Recordable[]>([]);
const isCreate = ref<boolean>(false);
getData();
function getData() {
  getTopicList().then(data => {
    topicList.value = data;
  });
}

function close() {
  isCreate.value = false;
  getData();
}
</script>

<style scoped lang="scss"></style>
