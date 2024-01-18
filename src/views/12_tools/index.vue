<template>
  <div class="page-wrapper">
    <MainContentHeader :title="routeTitle"></MainContentHeader>
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
    computed: {
      routeTitle() {
        let result = this.$t("route.tool");
        let YonghongBI = this.$t('docs.tools.yonghongbi.name')

        if (this.$route.params?.lang) {
          if (['YonghongBI'].includes(this.$route.params.lang)) {
            result += " - " + YonghongBI;
          } else {
            result += " - " + this.$route.params.lang;
          }
        }
        return result;
      },
    },
  };
</script>

<style lang="scss" scoped></style>
