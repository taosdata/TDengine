<template>
  <div class="detail">
    <el-button type="text" @click="back" icon="el-icon-arrow-left">{{
      $t("back")
    }}</el-button>
    <section class="content">
      <h1 class="title">
        {{ current.title }}
      </h1>
      <p class="subTitle">
        <span>{{ $t("dashboard.create") }}：{{ current.create_time }}</span>
        <span>{{ $t("support.updateTime") }}：{{ current.update_time }}</span>
      </p>
      <div class="support-detail">
        <div class="detail-item">
          {{ $t("support.supportState") }}：{{ current.state }}
        </div>
        <div class="detail-item">
          {{ $t("support.supportType") }}：{{ issueTypeObj[current.type] }}
        </div>
      </div>
      <EditorContentDisplay :content="current.description" />
      <el-tabs v-model="active">
        <el-tab-pane label="评论" name="comment">
          <comment :list="commments" />
        </el-tab-pane>
        <el-tab-pane label="附件" name="file">
          <fileC :list="attachmentList" />
        </el-tab-pane>
      </el-tabs>
    </section>
  </div>
</template>

<script>
import comment from "../components/comments.vue";
import fileC from "../components/file.vue";
import { queryIssue } from "@/api/gateway/support";
import { getToken } from "@/utils/token";
import EditorContentDisplay from "../components/editorContentDisplay.vue";
const imageType = ["BMP", "TIFF", "GIF", "PNG", "JPEG", "JPG", "WEBP"];
export default {
  props: {
    id: {
      type: String,
      default: "",
    },
  },
  data() {
    this.fileUrl = process.env.VUE_APP_FILE_URL;
    return {
      current: {},
      commments: [],
      attachmentList: [],
      active: "comment",
    };
  },
  components: { comment, fileC, EditorContentDisplay },
  provide() {
    return {
      detail: this,
    };
  },
  created() {
    this.getData();
  },
  computed: {
    issueTypeObj() {
      let obj = {};
      this.$store.state.issues.issuetype_list.forEach((item) => {
        obj[item.value] = item.label;
      });
      return obj;
    },
  },
  watch: {
    id() {
      this.getData();
    },
  },
  methods: {
    async getData() {
      let data = await queryIssue(this.id).catch(() => false);
      if (data) {
        this.current = data.workOrder || {};
        this.commments = data.commments || [];
        let conmmentsObj = {};
        this.commments.forEach((item) => {
          conmmentsObj[item.id] = [];
        });

        let token = getToken().split(" ")[1];
        this.attachmentList =
          data.attachmentList?.map((item) => {
            let type = item.name.split(".").slice(-1)[0] + "";
            if (type && imageType.includes(type.toUpperCase())) {
              item.type = "image";
            } else {
              item.type = "other";
            }
            item.url = this.fileUrl + item.url;
            item.url += "?Bearer=" + token;
            if (item.commentId) {
              conmmentsObj[item.commentId].push(item);
            }
            return item;
          }) || [];
        this.commments.forEach((item) => {
          item.attach = conmmentsObj[item.id];
        });
      }
    },
    back() {
      this.$router.push("/support");
    },
  },
};
</script>

<style lang="scss" scoped>
$nomral-text-color: #d4d5db;
$content-color: #b2b3c3;
.detail {
  padding: 0 20px;
}
.content {
  @include content-padding;
}
.title {
  font-size: 34px;
  .id {
    padding-left: 10px;
    color: $nomral-text-color;
    font-weight: normal;
  }
}
.subTitle {
  margin-top: 20px;
  font-size: 14px;
  color: $nomral-text-color;
  span + span {
    padding-left: 15px;
  }
}
.support-detail {
  font-size: 14px;
  color: $content-color;
  display: flex;
  flex-wrap: wrap;
  align-content: center;
  align-items: center;
  .detail-item {
    margin-top: 15px;
    @extend .flexCenter;
    & + .detail-item {
      margin-left: 15px;
    }
  }
}
.desc {
  text-indent: 2em;
  font-size: 16px;
  margin: 20px 0;
}
</style>
