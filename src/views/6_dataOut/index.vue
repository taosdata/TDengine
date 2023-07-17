<template>
  <div class="page-wrapper">
    <MainContentHeader :title="routeTitle"> </MainContentHeader>
    <section class="content">
      <!-- <router-view></router-view> -->
      <mainComponent></mainComponent>
    </section>
  </div>
</template>

<script>
import mainComponent from './views/main.vue'
export default{
  components: {mainComponent},
  computed: {
      routeTitle() {
        let result = this.$t("route.dataOut");

        if (this.$route.params?.lang) {
          result += " - " + this.$route.params.lang;
        }
        return result;
      },
    },
}
</script>

<style lang="scss" scoped></style>
