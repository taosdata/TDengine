<template>
  <div id="app">
    <router-view :key="key"></router-view>
  </div>
</template>

<script>
export default {
  name: "App",
  components: {},
  computed: {
    key() {
      return this.$store.state.app.current_cluster?.id || "";
    }
  },
  mounted() {
    console.log(
      "app",
      process.env.VUE_APP_CUS_NAME,
      !process.env.VUE_APP_CUS_NAME
    );

    this.$nextTick(() => {
      if (
        process.env.VUE_APP_CUS_NAME &&
        process.env.VUE_APP_CUS_NAME !== "TDengine"
      ) {
        //是oem需要单独处理
        let link =
          document.querySelector("link[rel*='icon']") ||
          document.createElement("link");
        let title = document.querySelector("title");
        title.innerText = process.env.VUE_APP_CUS_NAME;
        link.remove();
      }
    });
  }
};
</script>

<style lang="scss" scoped>
#app :deep(.CodeMirror-placeholder) {
  color: #c0c4cc;
}
</style>
