export default function () {
  const timeCount = ref(0);

  // resume - 继续，pause - 暂停， isActive - 是否执行中(isActive.value)
  const { resume, pause, isActive } = useIntervalFn(
    () => {
      timeCount.value--;
      console.log('开启了定时器', timeCount.value);
      if (timeCount.value === 0) {
        pause();
      }
    },
    1000,
    { immediate: false }
  );

  const start = (startTime = 60) => {
    if (isActive.value) return;
    // 因为初始值已经重置,所以继续执行,可以理解为 重新开始
    timeCount.value = startTime;
    resume();
  };

  // 页面要用到的数据，都返回
  return { timeCount, resume, pause, isActive, start };
}
