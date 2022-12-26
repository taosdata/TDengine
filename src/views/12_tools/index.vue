<template>
  <div class="page-wrapper">
    <MainContentHeader :title="$t('route.tool')"></MainContentHeader>
    <div class="content">
      <router-view />
    </div>
  </div>
</template>

<script>
  export default {
    data() {
      return {};
    },
  };
</script>

<style lang="scss" scoped></style>
