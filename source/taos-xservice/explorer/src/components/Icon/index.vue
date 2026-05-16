<template>
  <svg :style="{ fill: color ? color : '' }">
    <use :xlink:href="'#icon-' + name" />
  </svg>
</template>

<script>
export default {
  props: {
    name: {
      type: String,
      default: ''
    },
    color: {
      type: String,
      default: ''
    }
  }
};
</script>
