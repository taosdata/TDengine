<template>
  <div class="page-wrapper profile">
    <PageHeader :title="$t('login.profile')"> </PageHeader>
    <section class="content">
      <ChangePass class="form-style" :class="{ edit: isPassword }" @close="isPassword = false" />
    </section>
  </div>
</template>

<script setup lang="ts">
import ChangePass from './components/changePassword.vue';
const isPassword = ref<boolean>(false);
</script>

<style lang="scss" scoped>
.profile {
  &:deep(.el-form-item) {
    margin-bottom: 10px;
  }

  &:deep(.edit .el-form-item) {
    margin-bottom: 20px;
  }

  &:deep(.el-input.is-disabled .el-input__inner) {
    color: #16191f;
  }
}

.form-style {
  width: 600px;

  &:deep(.el-form-item__label) {
    font-weight: 500;
  }
}
</style>
