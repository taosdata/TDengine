<template>
  <div class="record_item">
    <pre
      @click="selectSQL"
      v-highlight.noCopy
    ><code class="language-sql" v-text="sql_sketchy"></code></pre>
    <div class="btn">
      <i :title="$t('copy')" @click="copy" class="el-icon-copy-document"></i>
      <!-- <i
        v-if="!isShared"
        :title="$t('share')"
        @click="addSharedFavorite"
        class="el-icon-share"
      ></i> -->
      <i class="el-icon-delete" :title="$t('del')" @click.stop="del"></i>
    </div>
  </div>
</template>

<script>
import { copy } from "@/utils";
// import { delFavorite, addSharedFavorite, delSharedFavorite } from "@/api/gateway/console";
export default {
  props: {
    record: {
      type: Object,
      default: () => {},
    },
    isShared: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      requestIng: false,
    };
  },
  computed: {
    sql_sketchy() {
      return this.record.sql;
    },
  },
  methods: {
    selectSQL() {
      // this.$store.commit('console/SET_SELECTED_RECORD', { rawSQL: this.record, parsedSQL: this.parsedSQL })
      this.$store.state.console.sqlStr +=
        (this.$store.state.console.sqlStr ? "\n" : "") + this.sql_sketchy;
    },
    copy() {
      copy(this.sql_sketchy);
    },
    async del() {
      if (this.requestIng) return;
      this.$confirm(
        this.$t("console.delFavirote") + ": " + this.sql_sketchy + "?",
        this.$t("tips"),
        {
          confirmButtonText: this.$t("confirm"),
          cancelButtonText: this.$t("cancel"),
          type: "warning",
        }
      )
        .then(async () => {
          this.requestIng = true;

          // let delSharedFavorite=
          let fn = this.isShared ? this.delSharedFavorite : this.delFavorite;
          await fn(this.record.id)
            .then(() => this.$message.success(this.$t("operateSucc")))
            .catch(() => this.$error(this.$t("operateFail")));
          // this.isShared ? await this.$store.dispatch("console/getSharedFavorites") : await this.$store.dispatch("console/getFavorites");
          this.requestIng = false;
        })
        .catch(() => {});
    },
    delSharedFavorite() {
      let shared = JSON.parse(localStorage.getItem("shared_favorites"));
      let index = shared.findIndex((item) => item.id === this.record.id);
      shared.splice(index, 1);
      localStorage.setItem("shared_favorites", JSON.stringify(shared));
      this.$store.commit(
        "console/SET_SHAREDFAVOURTIE",
        JSON.parse(localStorage.getItem("shared_favorites"))
      );
    },
    delFavorite() {
      let favorites = JSON.parse(localStorage.getItem("favorite_record"));
      let index = favorites.findIndex((item) => item.id === this.record.id);
      favorites.splice(index, 1);
      localStorage.setItem("favorite_record", JSON.stringify(favorites));

      this.$store.commit(
        "console/SET_FAVORITE",
        JSON.parse(localStorage.getItem("favorite_record"))
      );
    },
    addSharedFavorite() {
      if (this.requestIng) return;
      this.$confirm(
        this.$t("console.addSharedFavirote") + ": " + this.sql_sketchy + "?",
        this.$t("tips"),
        {
          confirmButtonText: this.$t("confirm"),
          cancelButtonText: this.$t("cancel"),
          type: "warning",
        }
      )
        .then(async () => {
          this.requestIng = true;
          // await addSharedFavorite(this.sql_sketchy)
          //   .then(() => this.$message.success(this.$t("shareSucc")))
          //   .catch(() => this.$error(this.$t("shareFail")));
          // await this.$store.dispatch("console/getSharedFavorites");
          if (localStorage.getItem("shared_favorites")) {
            let shared = JSON.parse(localStorage.getItem("shared_favorites"));
            shared.push(this.record);
            localStorage.setItem("shared_favorites", JSON.stringify(shared));
          } else {
            localStorage.setItem(
              "shared_favorites",
              JSON.stringify([].concat(this.record))
            );
          }
          this.$store.commit(
            "console/SET_SHAREDFAVOURTIE",
            JSON.parse(localStorage.getItem("shared_favorites"))
          );
          this.requestIng = false;
        })
        .catch(() => {});
    },
  },
};
</script>

<style lang="scss" scoped>
$height: 30px;
.record_item {
  font-size: 16px;
  padding-left: 10px;
  line-height: $height;
  // height: $height;
  display: flex;
  align-items: center;
  cursor: pointer;
  position: relative;
  font-family: Menlo, Monaco, Consolas, "Liberation Mono", "Courier New",
    monospace;
  code {
    line-height: $height;
    white-space: normal;
  }
  .btn {
    padding: 0 10px;
    font-size: 14px;
    position: absolute;
    right: 0;
    display: none;
    color: $color-primary;
    align-items: center;
    height: 100%;
    background-color: #fff;
    & > i + i {
      margin-left: 10px;
      cursor: pointer;
    }
  }
  & + .record_item {
    margin-top: 10px;
  }
}
.record_item:hover {
  background-color: #efefef;
  .btn {
    display: flex;
  }
}
</style>
