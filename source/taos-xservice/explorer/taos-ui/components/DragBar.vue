<template>
  <div :id="barId" class="drag-bar"></div>
</template>

<script lang="ts" setup>
import { throttle } from 'lodash-es';
const props = withDefaults(
  defineProps<{
    drapcolor?: string;
    draphovercolor?: string;
    barId?: string;
    leftId?: string;
    rightId?: string;
    mode?: 'horizontal' | 'vertical';
    modifyMax?: boolean;
  }>(),
  {
    drapcolor: '#f5f5f5',
    draphovercolor: '#dcdfe6',
    barId: 'drag-bar',
    leftId: '',
    rightId: '',
    mode: 'horizontal',
    modifyMax: false
  }
);
const { drapcolor, draphovercolor } = toRefs(props);
const emits = defineEmits(['changeWidth']);
const domPosKey = props.mode === 'horizontal' ? 'clientX' : 'clientY';
const offsetKey = props.mode === 'horizontal' ? 'clientWidth' : 'clientHeight';
const changeKey = props.mode === 'horizontal' ? 'width' : 'height';
const changeMaxKey = props.mode === 'horizontal' ? 'max-width' : 'max-height';
const width = props.mode === 'horizontal' ? '10px' : '100%';
const height = props.mode === 'horizontal' ? '100%' : '10px';

const cursor = props.mode === 'horizontal' ? 'ew-resize' : 'ns-resize';
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
    ev.preventDefault();
    ev.stopPropagation();

    const disW = changeEL[offsetKey];
    const disX = ev[domPosKey];

    // 保存原始的最大高度/宽度设置
    const originalMaxSize = changeEL.style[changeMaxKey];

    // 如果是垂直模式且操作的是rightId，清除最大高度限制
    if (props.mode === 'vertical' && rightId) {
      changeEL.style.maxHeight = 'none';
    }

    // 如果是水平模式且操作的是rightId，清除最大宽度限制
    if (props.mode === 'horizontal' && rightId) {
      changeEL.style.maxWidth = 'none';
    }

    // 添加拖拽状态的视觉反馈
    document.body.style.cursor = cursor;
    document.body.style.userSelect = 'none';

    const handleMouseMove = throttle((ev: MouseEvent) => {
      ev.preventDefault();
      let changeX;
      if (leftId) {
        changeX = ev[domPosKey] - disX;
      } else if (rightId) {
        changeX = disX - ev[domPosKey];
      }

      // 添加最小值限制，但去掉最大值限制
      const newSize = disW + changeX;
      const minSize = 100; // 最小尺寸

      if (newSize >= minSize) {
        changeEL.style[changeKey] = newSize + 'px';
        if (props.modifyMax) {
          changeEL.style[changeMaxKey] = newSize + 'px';
        }
        emits('changeWidth', changeEL.style[changeKey]);
      }
    }, 16);

    const handleMouseUp = () => {
      // 清理事件监听器
      document.removeEventListener('mousemove', handleMouseMove);
      document.removeEventListener('mouseup', handleMouseUp);

      // 恢复默认样式
      document.body.style.cursor = '';
      document.body.style.userSelect = '';

      const currentSize = changeEL.style[changeKey];
      const currentSizeValue = parseInt(currentSize);
      const originalMaxSizeValue = parseInt(originalMaxSize);

      if (currentSize) {
        if (originalMaxSize && currentSizeValue < originalMaxSizeValue) {
          // 如果当前尺寸小于原始最大尺寸，恢复原始设置
          changeEL.style[changeMaxKey] = originalMaxSize;
        } else {
          // 否则设置当前尺寸为最大值
          changeEL.style[changeMaxKey] = currentSize;
        }
      }
    };

    // 使用 addEventListener 而不是直接赋值，更可靠
    document.addEventListener('mousemove', handleMouseMove);
    document.addEventListener('mouseup', handleMouseUp);
    return false;
  };
}
</script>

<style lang="scss" scoped>
.drag-bar {
  width: v-bind(width);
  height: v-bind(height);
  cursor: v-bind(cursor);
  background-color: v-bind(drapcolor); // only can use lower charactoer for css parameter
}

.drag-bar:hover {
  background-color: v-bind(draphovercolor);
}
</style>
