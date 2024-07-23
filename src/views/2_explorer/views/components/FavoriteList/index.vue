<template>
  <el-row class="favorites_wrapper" id="favorites_wrapper" :gutter="20">
    <el-tabs type="border-card" size="mini" v-model="activeTab">
      <el-form
        :inline="true"
        size="small"
        label-position="left"
        style="position: absolute"
        @submit.native.prevent
      >
        <el-form-item prop="sql_desc_fuzzy">
          <el-input
            v-model="sqlDescFuzzy"
            clearable
            :placeholder="'SQL' + '/' + $t('console.desc')"
            @keyup.enter.native="getFavoritesData('search')"
            @clear="getFavoritesData('search')"
            style="width: 200px"
          />
        </el-form-item>
        <el-form-item>
          <el-button icon="el-icon-search" @click="getFavoritesData('search')">{{
            $t("search")
          }}</el-button>
        </el-form-item>
      </el-form>
      <el-tab-pane name="personal" :label="$t('console.persionalFavorites')">
        <el-table
          style="margin-top: 20px"
          :data="favorites"
          size="mini"
          row-key="id"
          max-height="calc(100% - 80px)"
          @cell-click="selectSQL"
        >
          <el-table-column label="SQL" prop="sql" min-width="180">
            <template slot-scope="scope">
              <el-tooltip
                placement="left-start"
                :open-delay="1000"
                effect="light"
              >
                <span slot="content">
                  <pre
                    v-highlight.noCopy
                    class="my-popper sql-code pre-code"
                    slot="reference"
                  >
                        <code class="language-sql" style="overflow:hidden">{{ scope.row.sql }} </code>
                      </pre>
                </span>
                <copy-text :text="scope.row.sql" isShowBtnText></copy-text>
              </el-tooltip>
            </template>
          </el-table-column>
          <el-table-column
            :label="$t('console.desc')"
            prop="description"
            width="310"
          >
          </el-table-column>
          <el-table-column :label="$t('topic.action')" width="140">
            <template slot-scope="scope">
              <el-tooltip effect="light" :content="$t('edit')" placement="top">
                <el-button
                  class="mini-btn"
                  size="mini"
                  @click="edit(scope.row)"
                  icon="el-icon-edit"
                ></el-button>
              </el-tooltip>
              <el-tooltip
                effect="light"
                :content="$t('console.share')"
                placement="top"
              >
                <el-button
                  class="mini-btn"
                  size="mini"
                  @click="manage(scope.row)"
                  icon="el-icon-share"
                  v-if="!scope.row.is_public"
                ></el-button>
              </el-tooltip>
              <el-tooltip
                effect="light"
                :content="$t('console.unshare')"
                placement="top"
              >
                <el-button
                  v-if="scope.row.is_public"
                  class="mini-btn"
                  size="mini"
                  @click="manage(scope.row)"
                  icon="el-icon-refresh-left"
                ></el-button>
              </el-tooltip>
              <el-tooltip
                effect="light"
                :content="$t('delete')"
                placement="top"
              >
                <el-button
                  plain
                  size="small"
                  @click="del(scope.row)"
                  icon="el-icon-delete"
                ></el-button>
              </el-tooltip>
            </template>
          </el-table-column>
        </el-table>
        <el-pagination
          class="pagination"
          layout="sizes, total, prev, pager, next"
          :current-page.sync="currentPage"
          :page-sizes="[10, 20, 50, 100, 200]"
          :page-size="pageSize"
          :hide-on-single-page="false"
          :total="total"
          @size-change="handleSizeChange"
          @current-change="handlePageChange"
        ></el-pagination>
      </el-tab-pane>
      <el-tab-pane name="shared" :label="$t('console.sharedFavorites')">
        <el-table
          style="margin-top: 20px"
          :data="sharedFavorites"
          size="mini"
          row-key="id"
          max-height="calc(100% - 80px)"
          @cell-click="selectSQL"
        >
          <el-table-column label="SQL" prop="sql" min-width="180">
            <template slot-scope="scope">
              <el-tooltip
                placement="left-start"
                :open-delay="1000"
                effect="light"
              >
                <span slot="content">
                  <pre
                    v-highlight.noCopy
                    class="my-popper sql-code pre-code"
                    slot="reference"
                  >
                        <code class="language-sql" style="overflow:hidden">{{ scope.row.sql }} </code>
                      </pre>
                </span>
                <copy-text :text="scope.row.sql" isShowBtnText></copy-text>
              </el-tooltip>
            </template>
          </el-table-column>
          <el-table-column
            :label="$t('console.desc')"
            prop="description"
            width="310"
          >
          </el-table-column>
          <el-table-column :label="$t('user')" prop="username" width="120" show-overflow-tooltip>
          </el-table-column>
          <el-table-column :label="$t('topic.action')" width="100">
            <template slot-scope="scope">
              <el-tooltip
                effect="light"
                :content="$t('console.addToPersonal')"
                placement="top"
              >
                <el-button
                  :disabled="scope.row.username == currentUserName"
                  class="mini-btn"
                  size="mini"
                  @click="add(scope.row)"
                  icon="el-icon-star-off"
                ></el-button>
              </el-tooltip>
              <el-tooltip
                effect="light"
                :content="$t('delete')"
                placement="top"
              >
                <el-button
                  :disabled="scope.row.username !== currentUserName"
                  plain
                  size="small"
                  @click="del(scope.row)"
                  icon="el-icon-delete"
                ></el-button>
              </el-tooltip>
            </template>
          </el-table-column>
        </el-table>
        <el-pagination
          class="pagination"
          layout="sizes, total, prev, pager, next"
          :current-page.sync="currentPageTwo"
          :page-sizes="[10, 20, 50, 100, 200]"
          :page-size="pageSizeTwo"
          :hide-on-single-page="false"
          :total="sharedTotal"
          @size-change="handleSizeChange"
          @current-change="handlePageChange"
        ></el-pagination>
      </el-tab-pane>
    </el-tabs>
  </el-row>
</template>

<script>
import { mapState } from "vuex";
import { copy } from "@/utils";
import {
  getFavorites,
  addFavorite,
  delFavorite,
  manageFavorite,
} from "@/api/gateway/console";
export default {
  components: {},
  computed: {
    ...mapState({
      favorites: (state) => state.console.favorites,
      total: (state) => state.console.total,
      sharedFavorites: (state) => state.console.sharedFavorites,
      sharedTotal: (state) => state.console.sharedTotal,
      selected_record: (state) => state.console.selected_record,
    }),
    currentUserName() {
      return localStorage.getItem("username");
    },
  },
  watch: {
    activeTab() {
      this.sqlDescFuzzy = "";
      this.getFavoritesData();
    },
  },
  data() {
    return {
      imageSize: Math.floor(window.innerHeight / 5),
      activeTab: "personal",
      currentPage: 1,
      pageSize: 20,
      currentPageTwo: 1,
      pageSizeTwo: 20,
      sqlDescFuzzy: "",
      maxHeight: "100%",
    };
  },
  methods: {
    pasteSQL(sql) {
      copy(sql);
    },
    async getFavoritesData(isSearch) {
      if (this.activeTab == "shared") {
        const params = {
          page: isSearch ? '1' : this.currentPageTwo,
          page_size: this.pageSizeTwo,
          sql_desc_fuzzy: this.sqlDescFuzzy,
          is_public: true,
        };
        this.$store.commit("console/SET_SHAREDFAVOURTIE", []);
        this.$store.dispatch("console/getSharedFavorites", params);
      } else {
        const params = {
          page: isSearch ? '1' : this.currentPage,
          page_size: this.pageSize,
          sql_desc_fuzzy: this.sqlDescFuzzy,
        };
        this.$store.commit("console/SET_FAVORITE", []);
        this.$store.dispatch("console/getFavorites", params);
      }
    },
    // 将别人共享的 SQL 添加到自己的空间下
    async add(row) {
      let params = {
        sql: row.sql,
        description: row.description,
      };
      const res = await addFavorite(params);
      if (res && res.code == 0) {
        this.$message.success(this.$t("operateSucc"));
        // 需要跳转到共享收藏tab吗？
        this.$store.dispatch("console/getFavorites", {
          page: 1,
          page_size: 20,
        });
      } else {
        this.$error(res.msg);
      }
    },
    async manage(row) {
      const { id, is_public } = row;
      const res = await manageFavorite(id, { public: !is_public });
      if (res && res.code == 0) {
        this.personalData = [];
        this.$message.success(this.$t("operateSucc"));
        this.getFavoritesData();
      } else {
        this.$error(res.msg);
      }
    },
    edit(row) {
      this.$prompt("", this.$t("console.editDesc"), {
        closeOnClickModal: false,
        confirmButtonText: this.$t("confirm"),
        cancelButtonText: this.$t("cancel"),
        inputPattern: /^.{0,20}$/,
        inputErrorMessage: this.$t("console.characterLen", ["20"]),
        inputPlaceholder: this.$t("console.descPlaceholder", ["20"]),
        inputValue: row.description,
        // center: true
      })
        .then(async ({ value }) => {
          this.favorited = true;
          let params = {
            description: value || "",
          };
          const res = await manageFavorite(row.id, params);
          if (res && res.code == 0) {
            this.$message.success(this.$t("changeSucc"));
            this.getFavoritesData();
          } else {
            this.$error(res.msg);
          }
          this.favorited = false;
        })
        .catch((err) => {
          console.log("error", err);
        });
    },
    async del(row) {
      const res = await delFavorite(row.id);
      if (res && res.code == 0) {
        this.$message.success(this.$t("delSucc"));
        this.getFavoritesData();
      } else {
        this.$error(res.msg);
      }
    },
    // 点击将 sql 添加到窗口中
    selectSQL(row, column, cell, event) {
      if (column.property === "sql") {
        this.$store.state.console.sqlStr +=
          (this.$store.state.console.sqlStr ? "\n" : "") + row.sql;
      }
    },
    handleSizeChange(val) {
      if (this.activeTab == "personal") {
        this.pageSize = val;
      } else {
        this.pageSizeTwo = val;
      }
      this.getFavoritesData();
    },
    handlePageChange() {
      this.getFavoritesData();
    },
  },
  mounted() {
    this.getFavoritesData();
  },
};
</script>

<style lang="scss" scoped>
.wrap {
  white-space: wrap;
}
.my-popper {
  white-space: wrap;
  max-width: 600px;
  max-height: 600px;
  overflow-y: auto;
  overflow-x: auto;
}
.favorites_wrapper {
  height: 100%;
  &:deep(.el-tab-pane) {
    top: 51px !important;
  }
  &:deep(.el-tabs--border-card) {
    box-shadow: none;
  }
  &:deep(.el-table) {
    display: flex;
    flex-direction: column;
    margin-top: 0 !important;
  }
  &:deep(.el-table__header-wrapper) {
    min-height: 30px;
  }
  &:deep(.el-tabs__content > .el-tab-pane) {
    height: 97%
  }
}
.copy-wrapper {
  line-height: normal !important;
}
</style>
