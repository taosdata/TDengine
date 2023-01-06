<template>
  <div class="page-wrapper">
    <MainContentHeader :title="$t('route.udf')"></MainContentHeader>
    <div class="content">
      <router-view />
    </div>
  </div>
</template>
<script>
export default {
  name: "UDF",
  data() {
    return {
      message: "这是UDF页面",
    };
  },
};
</script>