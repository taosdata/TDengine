<template>
  <div class="page-wrapper">
    <MainContentHeader :title="$t('route.sql')"></MainContentHeader>
    <div class="content">
      <router-view></router-view>
    </div>
  </div>
</template>

<script>
export default {
  components: {},
  data() {
    return {};
  },
  computed: {
    activeTab: {
      get() {
        return this.$route.name;
      },
      set(val) {
        this.$router.push({ name: val });
      },
    },
  },
  mounted() {},
  watch: {},
  methods: {},
};
</script>

<style lang="scss" scoped></style>
