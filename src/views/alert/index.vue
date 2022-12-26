<template>
  <div class="page-wrapper">
    <MainContentHeader :title="$t('route.alerts')"></MainContentHeader>
    <div class="content">
      <router-view></router-view>
    </div>
  </div>
</template>

<script>
export default {};
</script>

<style lang="scss" scoped></style>
