<template>
  <div class="page-wrapper">
    <MainContentHeader :title="$t('route.billing')"></MainContentHeader>
    <section class="content">
      <router-view></router-view>
    </section>
  </div>
</template>

<script>
export default {
  data() {
    return {};
  },
};
</script>

<style lang="scss"></style>
