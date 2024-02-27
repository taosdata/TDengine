<template>
  <div class="comment">
    <ul v-if="list.length">
      <li v-for="item in list" :key="item.id">
        <p class="title">{{ item.createdBy == "system" ? $t("support.system") : $t("support.self") }}:</p>
        <p class="content">{{ item.content }}</p>
        <div class="attach">
          <a v-for="ite in item.attach" :key="ite.id" :href="ite.url">{{ ite.name }}</a>
        </div>
        <p class="date">{{ parseTime(item.createTime) }}</p>
      </li>
    </ul>
    <el-empty v-else :image-size="200"></el-empty>
    <section class="btn">
      <el-button size="small" @click="dialog = true" type="primary">{{ $t("support.addComment") }}</el-button>
    </section>
    <!-- 添加评论 -->
    <el-dialog :title="$t('support.addComment')" width="1000px" :visible.sync="dialog" :close-on-click-modal="false">
      <addComment @close="dialogClose" />
    </el-dialog>
  </div>
</template>

<script>
import { parseTime } from "@/utils";
import addComment from "./addComment.vue";
export default {
  props: {
    list: {
      type: Array,
      default: () => [],
    },
  },
  data() {
    return {
      dialog: false,
    };
  },
  components: { addComment },
  methods: {
    parseTime(date) {
      return parseTime(date, "YYYY-MM-DD kk:mm:ss");
    },
    dialogClose() {
      this.dialog = false;
    },
  },
};
</script>

<style lang="scss" scoped>
.comment {
  margin-top: 20px;
  position: relative;
}
.title {
  font-size: 14px;
  font-weight: normal;
  color: #0052cc;
}
$border-color: #ddd;
ul {
  border-top: 1px solid $border-color;
  border-bottom: 1px solid $border-color;
  li {
    padding: 5px 20px;
    & + li {
      border-top: 1px solid $border-color;
    }
  }
  $padding-left: 20px;
  .date {
    padding-left: $padding-left;
    font-size: 12px;
    color: #5e6c84;
  }
  .content {
    padding-left: $padding-left;
    font-size: 14px;
    color: #172b4d;
  }
  .attach {
    padding-left: $padding-left;
    a {
      color: $color-primary;
      text-decoration: underline;
      & + a {
        margin-left: 20px;
      }
    }
  }
}
.btn {
  text-align: center;
  position: sticky;
  bottom: 0;
  left: 0;
  margin-top: 20px;
}
</style>
