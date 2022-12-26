import { debounce } from "@/utils";

export default {
    data() {
      return {
          $_resizeHandler: null
      }
    },
    beforeMount() {
      this.$_initResizeEvent()
    },
    beforeDestroy() {
      this.$_destroyResizeEvent()
    },
    activated() {
      this.$_initResizeEvent()
    },
    deactivated() {
      this.$_destroyResizeEvent()
    },
    mounted() {
      this.$_resizeHandler = debounce(() => {
        if (this.chart) {
            this.chart.resize()
        }
      }, 100)
      this.$_initResizeEvent()
    },
    methods: {
      // use $_ for mixins properties
      $_initResizeEvent() {
        window.addEventListener('resize', this.$_resizeHandler)
      },
      $_destroyResizeEvent() {
        window.removeEventListener('resize', this.$_resizeHandler)
      },
    }
}