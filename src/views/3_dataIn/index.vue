<template>
  <div class="page-wrapper">
    <MainContentHeader :title="routeTitle"></MainContentHeader>
    
    <section class="content" style="border:none!important;">
      <router-view></router-view>
    </section>
  </div>
</template>

<script>
import DataIn from './views/main.vue'
import DbSource from './views/dbSource.vue'
import DataCSV from './views/dataCSV.vue'
export default {
  components:{
    DataIn,
    DbSource,
    DataCSV
  },
  data() {
    return {};
  },
  computed: {
      routeTitle() {
        let result = this.$t("route.dataIn");

        if (this.$route.params?.lang) {
          result += " - " + this.$route.params.lang;
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
