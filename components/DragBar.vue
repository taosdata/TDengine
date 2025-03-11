<template>
  <div :id="barId" class="drag-bar"></div>
</template>

<script lang="ts" setup>
const props = withDefaults(
  defineProps<{
    drapcolor?: string;
    draphovercolor?: string;
    barId?: string;
    leftId?: string;
    rightId?: string;
  }>(),
  {
    drapcolor: '#f5f5f5',
    draphovercolor: '#dcdfe6',
    barId: 'drag-bar',
    leftId: '',
    rightId: ''
  }
);
const { drapcolor, draphovercolor } = toRefs(props);
const emits = defineEmits(['changeWidth']);
onMounted(() => {
  dragChangeWidth(props.barId, props.leftId, props.rightId);
});
function dragChangeWidth(barId: string, leftId: string, rightId: string) {
  const dragEl = document.getElementById(barId);
  let changeEL;
  if (leftId) {
    changeEL = document.getElementById(leftId);
  } else if (rightId) {
    changeEL = document.getElementById(rightId);
  }
  if (!dragEl || !changeEL) return;
  dragEl.onmousedown = ev => {
    const disW = changeEL.offsetWidth;
    const disX = ev.clientX;

    document.onmousemove = ev => {
      let changeX;
      if (leftId) {
        changeX = ev.clientX - disX;
      } else if (rightId) {
        changeX = disX - ev.clientX;
      }
      changeEL.style.width = disW + changeX + 'px';
      emits('changeWidth', changeEL.style.width);
    };
    document.onmouseup = () => {
      document.onmousemove = document.onmouseup = null;
    };
    return false;
  };
}
</script>

<style lang="scss" scoped>
.drag-bar {
  width: 10px;
  cursor: ew-resize;
  background-color: v-bind(drapcolor); // only can use lower charactoer for css parameter
}

.drag-bar:hover {
  background-color: v-bind(draphovercolor);
}
</style>
