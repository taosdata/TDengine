export default function (target: string, parent: string) {
  const maxHeight = ref('100%');
  onMounted(() => {
    const targetEl = document.querySelector(target) as HTMLElement;
    const parentEl = document.querySelector(parent) as HTMLElement;
    if (!targetEl || !parentEl) {
      console.log('找不到容器');
      return;
    }
    function setMaxHeight() {
      if (!parentEl || !targetEl) return;
      maxHeight.value = `${parentEl.clientHeight - targetEl.offsetTop}px`;
    }
    setMaxHeight();
    window.addEventListener('resize', setMaxHeight);
    onBeforeUnmount(() => {
      window.removeEventListener('resize', setMaxHeight);
    });
  });
  return maxHeight;
}
