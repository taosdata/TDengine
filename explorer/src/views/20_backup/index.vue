<template>
  <div class="page-wrapper">
    <MainContentHeader :title="$t('route.backup')"></MainContentHeader>
    <div class="content">
      <router-view />
    </div>
  </div>
</template>
<script>
export default {
  name: "Backup",
  data() {
    return {
      message: "这是Backup页面",
    };
  },
};
</script>