<template>
  <h1>{{ title }}</h1>
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
h1 {
  position: relative;
  margin-top: 10px;
  font-size: 24px;
  font-weight: bold;
  color: #343537;
  padding-left: 22px;
  &::before {
    width: 4px;
    height: 100%;
    background-color: $color-primary;
    position: absolute;
    left: 0;
    top: 0;
    content: "";
  }
}
</style>
