<template>
  <div class="page-wrapper">
    <MainContentHeader :title="$t('route.programming')"></MainContentHeader>
    <div class="content">
      <router-view />
    </div>
  </div>
</template>
<script>
export default {
  name: "Programming",
  data() {
    return {
      message: "这是Programming页面",
    };
  },
};
</script>