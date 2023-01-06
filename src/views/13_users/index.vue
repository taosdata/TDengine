<template>
  <div class="page-wrapper">
    <MainContentHeader :title="$t('route.users')"></MainContentHeader>
    <div class="content">
      <router-view />
    </div>
  </div>
</template>
<script>
export default {
  name: "Users",
  data() {
    return {
      message: "这是users页面",
    };
  },
};
</script>