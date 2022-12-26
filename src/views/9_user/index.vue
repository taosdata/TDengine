<template>
  <div class="page-wrapper">
    <MainContentHeader :title="$t('route.users')"></MainContentHeader>
    <section class="content">
      <router-view></router-view>
    </section>
  </div>
</template>

<script>
export default {};
</script>

<style lang="scss" scoped></style>
