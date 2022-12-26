<template>
  <div class="detail">
    <h1 class="title">
      {{ info.title }}
    </h1>
    <div class="other-info">
      <div class="label">
        <!-- <p>{{ $t("type") }}:</p> -->
        <p>{{ $t("time") }}:</p>
      </div>
      <div class="value">
        <!-- <p>{{ info.type }}</p> -->
        <p>{{ info.createTime }}</p>
      </div>
    </div>
    <el-divider></el-divider>
    <div class="message" v-html="info.message"></div>
  </div>
</template>

<script>
  export default {
    props: {
      info: {
        type: Object,
        default: () => ({}),
      },
    },
    data() {
      return {};
    },
    computed: {
      messageTypeObj() {
        return {
          1: this.$t("alert.cpuAlert"),
          2: this.$t("alert.memAlert"),
          3: this.$t("alert.diskAlert"),
        };
      },
    },
    methods: {},
  };
</script>

<style lang="scss" scoped>
  .detail {
    padding: 0 20px;
  }
  .title {
    font-size: 18px;
    font-weight: bold;
    text-align: center;
  }
  .other-info {
    display: flex;
    margin-top: 20px;
    .value {
      flex: 1;
    }
    .label {
      flex-shrink: 0;
      text-align: right;
      margin-right: 5px;
    }
  }
  .message {
    overflow: auto;
    padding-bottom: 20px;
  }
</style>
