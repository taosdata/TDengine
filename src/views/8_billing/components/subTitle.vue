<template>
  <div class="subtitle">
    {{ title }}
  </div>
</template>

<script>
export default {
  props: {
    title: {
      type: String,
      default: ""
    }
  }
};
</script>

<style lang="scss" scoped>
.subtitle {
  font-size: 14px;
  font-weight: bold;
}
</style>
