<template>
  <div class="page-wrapper">
    <MainContentHeader :title="routeTitle"></MainContentHeader>
    
    <section class="content" style="border:none!important;">
      <router-view></router-view>
    </section>
  </div>
</template>

<script>
export default {
  data() {
    return {};
  },
  computed: {
      routeTitle() {
        let result = this.$t("route.dataIn");

        if (this.$route.params?.lang) {
          result += " - " + this.$t(`docs.party.${this.$route.params.lang}.title`)
          // result += " - " + this.$route.params.lang;
        }
        return result;
      },
    },
  watch: {},
  methods: {},
};
</script>

<style lang="scss" scoped>
::v-deep.el-form-item__content {
  margin-left: 0px !important;
}
.content{
  border: none!important;
}
</style>
