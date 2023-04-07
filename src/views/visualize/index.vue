<template>
  <div class="page-wrapper">
    <MainContentHeader :title="routeTitle"> </MainContentHeader>
    <section class="content">
      <router-view></router-view>
    </section>
  </div>
</template>

<script>
export default{
   computed: {
      routeTitle() {
        let result = this.$t("route.visualize");

        if (this.$route.params?.lang) {
          result += " - " + this.$route.params.lang;
        }
        return result;
      },
    },
}
</script>

<style lang="scss" scoped></style>
