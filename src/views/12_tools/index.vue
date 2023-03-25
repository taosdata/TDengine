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

        if (this.$route.params?.lang) {
          result += " - " + this.$route.params.lang;
        }
        return result;
      },
    },
  };
</script>

<style lang="scss" scoped></style>
