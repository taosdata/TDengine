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
  name: "Programming",
  computed: {
      routeTitle() {
        let result = this.$t("route.programming");

        if (this.$route.params?.lang) {
          result += " - " + this.$route.params.lang;
        }
        return result;
      },
    },
  data() {
    return {
      message: "这是Programming页面",
    };
  },
};
</script>