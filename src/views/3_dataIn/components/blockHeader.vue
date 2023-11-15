<template functional>
  <div class="block-header">
    {{ props.title }}
    <slot></slot>
  </div>
</template>

<script>
export default {
  props: ['title'],
  components: {},
  data() {
    return {};
  },
  computed: {},
  watch: {},
  created() {},
  mounted() {},
  methods: {}
};
</script>

<style scoped lang="scss">
.block-header {
  font-size: 16px;
  color: #4259ce;
  font-weight: 600;
}
</style>
