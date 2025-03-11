import { EChartsType, init } from 'echarts';
import { useResizeObserver, useIntersectionObserver } from '@vueuse/core';
import { debounce } from 'lodash-es';

// 事件监听回调列表
const eventList = ['click', 'finished', 'mouseover', 'mousemove', 'updateAxisPointer'];
export default defineComponent({
  props: {
    option: {
      type: Object,
      required: true
    },
    height: {
      type: String,
      default: '400px'
    },
    width: {
      type: String,
      default: '100%'
    },
    svg: {
      type: Boolean,
      default: false
    }
  },
  emits: eventList.concat(['chartMounted']),
  setup(props, ctx) {
    const chartRef = shallowRef<HTMLElement | null>(null);
    let instance: EChartsType | null = null;
    const targetIsVisible = ref(false);
    let isNeedSetOption = false;
    onMounted(() => {
      initChart();
      useResizeObserver(
        chartRef,
        debounce(entries => {
          const entry = entries[0];
          const { width, height } = entry.contentRect;
          instance?.resize({ width, height });
        }, 500)
      );
      const { stop } = useIntersectionObserver(chartRef, ([{ isIntersecting }]) => {
        targetIsVisible.value = isIntersecting;
      });
      onBeforeUnmount(() => {
        stop();
      });
    });
    onBeforeUnmount(() => {
      if (instance) {
        instance.clear();
        instance.dispose();
        instance = null;
      }
    });
    watch(
      () => props.option,
      () => {
        isNeedSetOption = true;
        if (instance) {
          setOptions();
        } else {
          initChart();
        }
      },
      { deep: true }
    );
    watch(targetIsVisible, value => {
      if (value && isNeedSetOption) {
        setOptions();
      }
    });
    function initChart() {
      if (!chartRef.value) {
        return;
      }
      const params: Parameters<typeof init> = [chartRef.value];
      if (props.svg) {
        params.push(null, { renderer: 'svg' });
      }
      instance = init(...params);
      eventList.forEach((event: string) => {
        instance!.on(event, (...rest: unknown[]) => {
          ctx.emit(event, ...rest);
        });
      });
      ctx.emit('chartMounted', instance);
      setOptions();
    }
    function setOptions() {
      if (!instance) {
        return;
      }
      instance.setOption(props.option ?? {}, {
        notMerge: true,
        lazyUpdate: true
      });
      instance.resize();
      isNeedSetOption = false;
    }

    return () =>
      h('div', {
        ...ctx.attrs,
        ref: el => (chartRef.value = el as HTMLDivElement),
        style: { width: props.width, height: props.height }
      });
  }
});
