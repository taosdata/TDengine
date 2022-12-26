import { debounce } from "@/utils";

export default {
    data() {
      return {
          $_resizeHandler: null
      }
    },
    computed: {
      opened() {
        return this.$store.state.sidebar.opened 
      }
    },
    beforeMount() { 
      this.$_initResizeEvent()
    },
    beforeDestroy() {
      this.$_destroyResizeEvent()
    },
    mounted() {
      this.$_resizeHandler = debounce(() => {
        const rect = document.body.getBoundingClientRect()
        if(rect.width <= 1200 && this.opened){
          this.$store.commit('sidebar/TOGGLE_SIDEBAR')
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