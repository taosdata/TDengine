<template>
  <section class="title-wrapper">
    <h1 class="title">
      {{ title }}
    </h1>
    <slot></slot>
  </section>
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
.title-wrapper {
  display: flex;
  justify-content: space-between;
  align-items: center;
}
.title {
  font-size: 24px;
  line-height: 50px;
  font-weight: normal;
}
</style>
