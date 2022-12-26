<template>
  <div class="maincontent_header block-style">
    <div class="default">
      <div @click="goBack" class="maincontent_title">{{ title }}</div>
      <slot></slot>
    </div>
    <slot name="right"></slot>
  </div>
</template>

<script>
export default {
  props: ["title"],
  methods: {
    goBack() {
      this.$router.push(this.$route.matched[1]?.path || "/");
    }
  }
};
</script>

<style lang="scss" scoped>
.maincontent_header {
  height: 55px;
  padding: 0 30px;
  display: flex;
  justify-content: space-between;
  align-items: center;
  flex-shrink: 0;
  margin-bottom: 5px;
}
.default {
  display: flex;
  align-items: center;
}
.maincontent_title {
  height: 30px;
  line-height: 30px;
  font-size: 28px;
  font-weight: 700;
  color: $title-color;
  text-transform: capitalize;
  padding-left: 15px;
  border-left: 4px solid $color-primary;
  border-radius: 2px;
  cursor: pointer;
}
</style>
