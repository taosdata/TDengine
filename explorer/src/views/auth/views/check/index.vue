<template>
  <div class="checkEmail">
    <el-button
      style="width:100%;"
      @click="$router.push('/auth/login')"
      type="primary"
      >{{ $t("log-in") }}</el-button
    >
  </div>
</template>

<script>
export default {};
</script>

<style scoped>
.checkEmail {
  margin-top: 30px;
  text-align: center;
}
.checkTip {
  margin: 30px 0;
}
</style>
