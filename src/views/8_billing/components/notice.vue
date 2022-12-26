<template>
  <div class="notice">
    <el-form>
      <el-form-item label="登陆邮箱">
        <el-input v-model="info.email"></el-input>
      </el-form-item>
      <el-form-item :label="$t('phone')">
        <el-input v-model="info.phone"></el-input>
      </el-form-item>
      <el-form-item :label="$t('email')">
        <el-input v-model="info.email"></el-input>
      </el-form-item>
    </el-form>
  </div>
</template>

<script>
export default {
  data() {
    return {
      info: {
        email: "",
        phone: ""
      }
    };
  },
  computed: {
    email() {
      return this.$store.state.auth.userInfo.email;
    }
  }
};
</script>

<style></style>
