import { computed } from 'vue';
import { debounce } from 'lodash-es';
import { useStore } from 'vuex';

export function useResizeHandler() {
  const store = useStore();
  const opened = computed(() => store.state.sidebar.opened);

  const $_initResizeEvent = () => {
    window.addEventListener('resize', $_resizeHandler);
  };

  const $_destroyResizeEvent = () => {
    window.removeEventListener('resize', $_resizeHandler);
  };

  const $_resizeHandler = debounce(() => {
    const rect = document.body.getBoundingClientRect();
    if (rect.width <= 1200 && opened.value) {
      store.commit('sidebar/TOGGLE_SIDEBAR');
    }
  }, 100);

  return {
    $_initResizeEvent,
    $_destroyResizeEvent
  };
}
