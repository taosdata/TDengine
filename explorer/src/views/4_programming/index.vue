<template>
  <div class="page-wrapper">
    <!-- <PageHeader :title="routeTitle"></PageHeader> -->
    <div class="content">
      <router-view />
    </div>
  </div>
</template>
<script>
export default {
  name: 'Programming',
  data() {
    return {
      message: '这是Programming页面'
    };
  },
  computed: {
    routeTitle() {
      let result = this.$t('route.programming');

      if (this.$route.params?.lang) {
        result += ' - ' + this.$route.params.lang;
      }
      return result;
    }
  }
};
</script>
