<template>
  <div class="page-wrapper">
    <MainContentHeader :title="$t('route.dnodes')"></MainContentHeader>
    <div class="content">
      <router-view />
    </div>
  </div>
</template>
<script>
export default {
  name: "DNodes",
  data() {
    return {
      message: "这是DNodes页面",
    };
  },
};
</script>