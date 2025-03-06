<template>
    <p class="title">
        <span>{{ name }}</span>
        <el-button v-if="showAdd" class="btn-pos" link size="default" icon="Plus" @click.prevent="$emit('add')">{{
            $t('add')
        }}</el-button>
    </p>
</template>

<script setup lang="ts">
defineProps({
  name: {
    type: String,
    default: ''
  },
  showAdd: {
    type: Boolean,
    default: false
  }
});

defineEmits(['add']);
</script>

<style lang="scss" scoped>
.title {
  padding: 8px 16px;
  margin: 0 0 20px;
  line-height: 28px;
  font-size: 16px;
  color: #333;
  background-color: #ecf8ff;
  border-left: 5px solid #50bfff;
  border-radius: 4px;

  .btn-pos {
    float: right;
  }
}
</style>