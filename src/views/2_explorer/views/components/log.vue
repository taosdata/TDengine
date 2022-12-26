<template>
  <div class="log">
    <div ref="content" v-if="history.length" class="log-content">
      <RecordItem v-for="record in history" :key="record.createdAt" :record="record"></RecordItem>
    </div>
  </div>
</template>

<script>
  import RecordItem from "./recordItem";
  export default {
    components: { RecordItem },
    data() {
      return {};
    },
    computed: {
      history() {
        return this.$store.state.console.history;
      },
    },
    watch: {
      "history.length": {
        handler(val) {
          if (!val) return;
          this.$nextTick(() => {
            if (!this.$el.scrollHeight) return;
            this.$el.scrollTop = this.$el.scrollHeight;
          });
        },
        immediate: true,
      },
    },
  };
</script>

<style lang="scss" scoped>
  .log {
    display: block;
    height: 100%;
    overflow: hidden auto;
    border: 1px solid #dcdfe6;
  }
  .console_icon {
    width: 18px;
    height: 18px;
    flex-shrink: 0;
  }

  .title {
    margin-left: 10px;
  }
  .log-content {
    @include content-padding;
  }
  .expand-icon {
    position: absolute;
    right: 20px;
    width: 18px;
    height: 18px;
    transform: rotateZ(-90deg);
    transition: transform 0.3s ease;
    &.close {
      transform: rotateZ(90deg);
    }
  }
</style>
