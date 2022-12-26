<template>
  <div class="page-wrapper">
    <MainContentHeader :title="$t('route.dataIn')"></MainContentHeader>
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
    watch: {},
    methods: {},
  };
</script>

<style lang="scss"></style>
