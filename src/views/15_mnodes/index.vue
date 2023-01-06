<template>
  <div class="page-wrapper">
    <MainContentHeader :title="$t('route.mnodes')"></MainContentHeader>
    <div class="content">
      <router-view />
    </div>
  </div>
</template>
<script>
export default {
  name: "MNodes",
  data() {
    return {
      message: "这是MNodes页面",
    };
  },
};
</script>