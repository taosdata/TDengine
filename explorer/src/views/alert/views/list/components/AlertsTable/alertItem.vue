<template>
  <div class="alert-item" :class="{ unread: !alert.status }">
    <!-- <div class="type">
      {{ alert.messageType }}
    </div> -->
    <div class="title">
      {{ alert.title }}
    </div>
    <div class="message">
      {{ message }}
    </div>
    <div class="date">
      {{ alert.createTime }}
    </div>
  </div>
</template>

<script>
  import { HtmlToText } from "@/utils";
  export default {
    props: {
      alert: {
        type: Object,
        default: () => ({}),
      },
    },
    computed: {
      message() {
        return HtmlToText(this.alert?.message || "");
      },
    },
  };
</script>

<style lang="scss" scoped>
  .alert-item {
    display: flex;
    align-items: center;
    padding: 10px 0;
    font-size: 14px;
    line-height: 30px;
    color: #333;
    border-bottom: 1px solid #ebeef5;
    border-top: 1px solid #ebeef5;
    // font-weight: lighter;
    cursor: pointer;
    .title {
      width: 300px;
      flex-shrink: 0;
      @extend .nowrap;
    }
    .message {
      flex: 1;
      @extend .nowrap;
      font-weight: normal;
      color: #a0a0a0;
    }
    .date {
      @extend .nowrap;
      flex-shrink: 0;
      width: 150px;
      text-align: right;
    }
    .type {
      @extend .nowrap;
      flex-shrink: 0;
      width: 160px;
    }
    & + .alert-item {
      border-top: none;
    }
  }
  .unread {
    font-family: inherit;
    font-weight: bold;
    color: black;
  }
</style>
