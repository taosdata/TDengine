<template>
  <div class="page-wrapper landing">
    <MainContentHeader :title="$t('landing.title')"> </MainContentHeader>
    <section class="content">
      <div class="landing-content">
        <p class="primary-tip" v-html="$t('landing.introduce')"></p>
        <!-- <p class="success-tip" v-html="$t('landing.pageTip')"></p> -->
        <Tutorial />
      </div>
    </section>
  </div>
</template>

<script>
  import Tutorial from "./components/tutorial.vue";
  export default {
    components: { Tutorial },
  };
</script>

<style lang="scss" scoped>
  .landing-content {
    max-width: 1400px;
  }
</style>
