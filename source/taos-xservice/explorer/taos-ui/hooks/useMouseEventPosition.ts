// 获取鼠标事件距离元素的位置
export default function () {
  const x = ref(0);
  const y = ref(0);
  function handleEvent(e: MouseEvent) {
    const target = e.target as HTMLElement;
    const rect = target.getBoundingClientRect();
    x.value = e.clientX - rect.left;
    y.value = e.clientY - rect.top;
  }
  return { x, y, handleEvent };
}
