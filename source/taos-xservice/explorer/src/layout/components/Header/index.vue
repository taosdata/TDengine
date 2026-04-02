<template>
  <div class="header">
    <Version/>
    <div class="headerRight">
      <Timezone/>
      <Document v-if="docUrl"/>
      <!-- <International/> -->
      <Avatar/>
    </div>
  </div>
</template>

<script setup lang="ts">

import {
  Avatar,
  Document,
  Timezone,
  Version,
  // International,
} from "./components";
const { $IS_COMMUNITY } = inject("globalCustomProperties") as GlobalCustomProperties;

const docUrl = ref(localStorage.getItem("documentWebsite"))
 
onBeforeMount(() => {
  if ($IS_COMMUNITY) {
    docUrl.value = "";
  }
})

</script>

<style lang="scss" scoped>
.header {
  display: flex;
  flex-direction: row;
  justify-content: space-between;
  align-items: center;
  background-color: #fff;
  padding-right: 40px;
  padding-left: 40px;
  position: sticky;
  top: 0;
  z-index: 1;
  height: 58px;
  width: 100%;
  border-bottom: 1px solid #eaecef;
  flex-shrink: 0;
}

@keyframes blink {
  0% {
    opacity: 0;
  }
  50% {
    opacity: 1;
  }
  100% {
    opacity: 0;
  }
}
.headerRight {
  display: flex;
  flex-direction: row;
  align-items: center;
}

</style>
