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
      },
    },
  };
</script>

<style></style>
