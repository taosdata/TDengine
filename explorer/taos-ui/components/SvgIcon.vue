<template>
  <div v-if="isExternal" :style="styleExternalIcon" class="svg-external-icon svg-icon" />
  <svg v-else :class="svgClass">
    <use :xlink:href="iconName" />
  </svg>
</template>

<script setup lang="ts">
import { isExternal as isExternalLink } from '../utils/validate';

const props = defineProps({
  name: {
    type: String,
    required: true
  },
  class: {
    type: String,
    default: ''
  }
});

const isExternal = computed(() => {
  return isExternalLink(props.name);
});
const iconName = computed(() => {
  return `#icon-${props.name}`;
});
const svgClass = computed(() => {
  if (props.class) {
    return 'svg-icon ' + props.class;
  } else {
    return 'svg-icon';
  }
});
const styleExternalIcon = computed(() => {
  return {
    mask: `url(${props.name}) no-repeat 50% 50%`,
    '-webkit-mask': `url(${props.name}) no-repeat 50% 50%`
  };
});
</script>

<style scoped>
.svg-icon {
  display: inline-block;
  flex-shrink: 0;
  overflow: hidden;
  vertical-align: -0.15em;
  pointer-events: auto;
  cursor: pointer;
  fill: currentcolor;
}

.svg-external-icon {
  display: inline-block;
  background-color: currentcolor;
  mask-size: cover !important;
}
</style>
