<template>
  <div class="page-wrapper landing">
    <PageHeader :title="$t('landing.title')"> </PageHeader>
    <section class="content">
      <div class="landing-content">
        <p v-dompurify-html="$t('landing.introduce')" class="primary-tip"></p>
        <!-- <p class="success-tip" v-html="$t('landing.pageTip')"></p> -->
        <Tutorial />
      </div>
    </section>
  </div>
</template>

<script setup lang="ts">
import Tutorial from './components/tutorial.vue';
</script>

<style lang="scss" scoped>
.landing-content {
  max-width: 1400px;
}
</style>
