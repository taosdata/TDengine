<template>
  <div class="page-wrapper">
    <MainContentHeader :title="$t('route.qnodes')"></MainContentHeader>
    <div class="content">
      <router-view />
    </div>
  </div>
</template>
<script>
export default {
  name: "QNodes",
  data() {
    return {
      message: "这是QNodes页面",
    };
  },
};
</script>