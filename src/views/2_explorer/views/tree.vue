<template>
  <div class="dbs-tree">
    <PanelHeader style="justify-content: space-between">
      <div>
        <Icon name="database_icon" class="database_icon"></Icon>
        <span class="title">{{ $t("explorer.databases") }}</span>
      </div>
      <div>
        <el-button
          icon="el-icon-refresh"
          @click="refersh"
          size="mini"
        ></el-button>
        <el-button
          @click="addDatabase"
          size="mini"
          icon="el-icon-plus"
          plain
          v-permission
        ></el-button>
      </div>
    </PanelHeader>
    <div class="dbs-tree-container">
      <VueEasyTree
        lazy
        :key="treeKey"
        :empty-text="$t('data.noDatabase')"
        accordion
        highlight-current
        node-key="node-key"
        :load="loadNode"
        :props="props"
        :height="height"
        :default-expanded-keys="defaultExpandedKeys"
        @node-collapse="collapseChange"
        @node-expand="expandChange"
      >
        <el-tooltip
          slot-scope="{ node, data }"
          effect="dark"
          :open-delay="1000"
          :content="node.label"
          placement="right"
          popper-class="el-tree-popper"
        >
          <div class="tree-wrapper">
            <div class="console-custom-tree-node">
              <Icon
                :name="icon[data.typeName]"
                class="console-tree-icon"
              ></Icon>
              <span>{{ node.label }}</span>
            </div>
            <span v-if="data.dataType" class="column-type">{{
              data.dataType
            }}</span>
            <section class="operate-btn">
              <template v-if="!data.dataType">
                <span>{{ data.dataType }}</span>
                <el-tooltip
                  effect="light"
                  placement="top"
                  :open-delay="1000"
                  :content="getTooltip(data, 'view')"
                  v-if="
                    !['sfile', 'nfile', 'column', 'tag'].includes(data.typeName)
                  "
                >
                  <i
                    class="el-icon-view operate-icon"
                    @click.stop="view(data)"
                  ></i>
                </el-tooltip>
                <template v-if="!data.noOperate">
                  <!-- <el-tooltip
                    effect="light"
                    placement="top"
                    :content="getTooltip(data, 'search')"
                    v-if="['sfile', 'nfile', 'stable'].includes(data.typeName)"
                  >
                    <i
                      style="margin-right: 10px"
                      v-permission
                      class="el-icon-query"
                      @click.stop="search(data, node)"
                    ></i>
                  </el-tooltip> -->
                  <el-tooltip
                    effect="light"
                    placement="top"
                    :open-delay="1000"
                    :content="getTooltip(data, 'add')"
                    v-if="['sfile', 'nfile', 'stable'].includes(data.typeName)"
                  >
                    <i
                      v-permission
                      class="el-icon-plus operate-icon"
                      @click.stop="add(data, node)"
                    ></i>
                  </el-tooltip>

                  <el-tooltip
                    effect="light"
                    placement="top"
                    :open-delay="1000"
                    :content="getTooltip(data, 'edit')"
                    v-if="
                      !['sfile', 'nfile', 'column', 'tag'].includes(
                        data.typeName
                      )
                    "
                  >
                    <i
                      v-permission
                      class="el-icon-edit operate-icon"
                      @click.stop="edit(data, node)"
                      v-if="
                        !['sfile', 'nfile', 'column', 'tag'].includes(
                          data.typeName
                        )
                      "
                    ></i>
                  </el-tooltip>
                  <template v-if="isRoot === 'root'">
                    <el-tooltip
                      effect="light"
                      placement="top"
                      :open-delay="1000"
                      :content="getTooltip(data, 'manage')"
                      v-if="data.typeName === 'database'"
                    >
                      <i
                        v-permission
                        class="el-icon-unlock operate-icon"
                        @click.stop="manage(data, node)"
                        v-if="data.typeName === 'database'"
                      ></i>
                    </el-tooltip>
                  </template>
                  <el-tooltip
                    effect="light"
                    placement="top"
                    :open-delay="1000"
                    :content="getTooltip(data, 'del')"
                    v-if="
                      !['sfile', 'nfile', 'column', 'tag'].includes(
                        data.typeName
                      )
                    "
                  >
                    <i
                      v-permission
                      class="el-icon-delete operate-icon"
                      @click.stop="del(data, node)"
                      v-if="
                        !['sfile', 'nfile', 'column', 'tag'].includes(
                          data.typeName
                        )
                      "
                    ></i>
                  </el-tooltip>
                </template>
              </template>
              <el-tooltip
                v-if="data.typeName == 'table' || data.typeName == 'stable'"
                effect="light"
                :open-delay="1000"
                :content="$t('data.viewData')"
              >
                <div
                  v-if="data.typeName == 'table' || data.typeName == 'stable'"
                  class="tablebutton"
                  @click.stop="clickAdd(data, true)"
                >
                  <i class="el-icon-search"></i>
                </div>
              </el-tooltip>
              <el-tooltip
                effect="light"
                :content="$t('data.appendEditor')"
                :open-delay="1000"
              >
                <div
                  class="tablebutton"
                  @click.stop="clickAdd(data)"
                  v-if="!['sfile', 'nfile'].includes(data.typeName)"
                >
                  <svg
                    viewBox="0 0 24 24"
                    height="16px"
                    width="16px"
                    aria-hidden="true"
                    focusable="false"
                    fill="currentColor"
                    xmlns="http://www.w3.org/2000/svg"
                    class="StyledIconBase-ea9ulj-0 iKhrnw"
                  >
                    <path fill="none" d="M0 0h24v24H0z"></path>
                    <path
                      d="M24 12l-5.657 5.657-1.414-1.414L21.172 12l-4.243-4.243 1.414-1.414L24 12zM2.828 12l4.243 4.243-1.414 1.414L0 12l5.657-5.657L7.07 7.757 2.828 12zm6.96 9H7.66l6.552-18h2.128L9.788 21z"
                    ></path>
                  </svg>
                  <!-- <span>Add</span> -->
                </div>
              </el-tooltip>
            </section>
          </div>
        </el-tooltip>
      </VueEasyTree>
    </div>
    <el-dialog
      :title="dialogtitle"
      :visible.sync="searchdialog"
      width="40%"
      :destroy-on-close="true"
      @close="closeDialog"
      :close-on-click-modal="false"
    >
      <div class="tag-list">
        <div class="open-tag" v-if="showtag">
          <span class="label" style="width: 150px; margin-bottom: 0px">{{
            $t("data.enabletag")
          }}</span>
          <el-switch v-model="switchtag"> </el-switch>
        </div>
        <template v-if="switchtag">
          <TagColumn
            v-for="(item, index) in tagList"
            :key="index"
            :tagColumnData="item"
          ></TagColumn>
        </template>
      </div>
      <el-form :model="serachForm" ref="searchForm" label-width="120px">
        <!-- <el-form-item label="Tag" prop="tagname" v-if="showtag">
          <div class="tag-column">
             <el-select v-model="tagCondition" placeholder="请选择">
              <el-option
                v-for="item in conditionList"
                :key="item"
                :label="item"
                :value="item"
              >
              </el-option>
            </el-select>
             
          </div>
        </el-form-item> -->
        <el-form-item
          :label="$t('datasource.csvtable')"
          prop="tablename"
          :rules="tablerule"
        >
          <el-input v-model="serachForm.tablename" size="small"></el-input>
        </el-form-item>
      </el-form>
      <div class="footer">
        <el-button @click="closeDialog" size="small">{{
          $t("cancel")
        }}</el-button>
        <el-button type="primary" @click="searchTables">{{
          $t("confirm")
        }}</el-button>
      </div>
    </el-dialog>
  </div>
</template>

<script>
import { getDBListReq, getDBStruct } from "@/api/gateway/data/dbs.js";
import {
  getStableListReq,
  getAllNormalTables,
  getStableStructReq,
} from "@/api/gateway/data/stables.js";
import {
  getTableListReq,
  getMatrixStructReq,
  getTableStructReq,
} from "@/api/gateway/data/tables.js";
import { sendSQLReq } from "@/api/gateway/console";
import PanelHeader from "./components/panelHeader.vue";
import VueEasyTree from "@/components/Tree";
import { deepClone } from "@/utils";
import moment from "moment";
import { Message } from "element-ui";
import TagColumn from "./components/tagColumn.vue";
import {
  CompareOperator,
  JsonOperator,
  GeneralOperator,
  RegularOperator,
} from "@/const";
import { getRunningTask } from "@/api/explorer/datain";
const clickNoChange = ["sql", "xterm"];
const getGeneralFn = (type) => {
  return GeneralOperator.filter((item) => !type.includes(item.label)).map(
    (item) => item.label
  );
};
const conditionMap = {
  TIMESTAMP: CompareOperator.concat(getGeneralFn(["TIMESTAMP"])),
  NUMBER: CompareOperator.concat(getGeneralFn(["NUMBER"])),
  STRING: RegularOperator.concat(getGeneralFn(["STRING"])),
  JSON: JsonOperator,
  BOOL: CompareOperator.concat(getGeneralFn(["NOT BETWEEN", "BETWEEN"])).concat(
    ["NOT BETWEEN", "BETWEEN"]
  ),
};
const conditionList = CompareOperator.concat(
  getGeneralFn(["NOT BETWEEN AND", "BETWEEN AND"])
);
export default {
  props: {
    addSql: {
      type: String,
      default: "",
    },
  },
  components: { PanelHeader, VueEasyTree, TagColumn },
  data() {
    this.icon = {
      database: "database_icon",
      stable: "stable",
      table: "table",
      sfile: "sfile",
      nfile: "nfile",
      column: "circle_blod",
      tag: "tag",
    };
    this.notAdd = ["column", "tag"];
    return {
      switchtag: false,
      tagList: [],
      conditionList,
      dialogtitle: "",
      searchdialog: false,
      showtag: false,
      currentSearch: "",
      serachForm: {
        tagname: "",
        tablename: "",
      },
      tagrule: [
        {
          required: false,
          message: this.$t("data.searchtbtip"),
          trigger: "blur",
        },
      ],
      tablerule: [
        {
          required: true,
          message: this.$t("data.searchtbtip"),
          trigger: "blur",
        },
      ],
      isRoot: localStorage.getItem("username"),
      defaultProps: {
        children: "children",
        label: "label",
      },
      val: "",
      resData: [],
      props: {
        label: "name",
        children: "children",
        isLeaf: "leaf",
      },
      height: "800px",
      requesting: false,
      defaultExpandedKeys: [],
      pageSize: Math.min(
        Math.max(Math.floor((window.innerHeight - 240) / 30), 5),
        10000
      ),
      isRequest: false,
    };
  },
  computed: {
    treeKey() {
      return this.$store.state.console.treeKey;
    },
  },
  mounted() {
    this.height = this.$el.clientHeight - 70 + "px";
  },
  methods: {
    closeDialog() {
      this.serachForm.tagname = "";
      this.serachForm.tablename = "";
      this.searchdialog = false;
    },
    refersh() {
      this.$store.commit("console/CHANGE_TREE_KEY");
    },
    async clickAdd(data, all) {
      if (all) {
        let columns = [];
        let db = "";
        if (data.typeName === "stable") {
          db = data.parent.split(".")[0];
          let sdata = await getStableStructReq({
            selected_db: db,
            stableName:
              data.typeName == "stable" ? data.name : data.parent.split(".")[1],
            type: 'crea'
          }).catch(() => ({
            ts_field_name: "",
            columns: [],
            tags: [],
          }));
          columns = sdata.columns.map((item) => `\`${item.field}\``);
        }
        if (data.typeName === "table") {
          db = data.stable_name ? data.parent.split(".")[0] : data.parent;
          let sdata = await getTableStructReq({
            selected_db: db,
            tableName:
              data.typeName == "table" ? data.name : data.parent.split(".")[1],
          }).catch(() => ({
            ts_field_name: "",
            columns: [],
            tags: [],
          }));
          columns = [`\`${sdata.ts_field_name}\``].concat(
            sdata.columns.map((item) => `\`${item.field}\``)
          );
        }

        this.$store.state.console.addSql =
          `${this.$store.state.console.sqlStr ? "\n" : ""}SELECT ${
            columns.join(",") || "*"
          } FROM  \`${db}\`` +
          "." +
          `\`${data.name}\` limit 200;`;
        this.$store.state.console.sqlStr += this.$store.state.console.addSql;
      } else {
        let code = data.parent
          ? `\`${data.parent.split(".")[0]}\`.\`${data.name}\``
          : `\`${data.name}\``;
        this.$store.state.console.addSql = " " + code + " ";
      }
      this.changePartActive();
    },
    addDatabase() {
      this.$store.commit("dbs/HANDLE_ADD_DB");
      this.$store.commit("console/SET_TAB_NAME", "add");
      this.$store.commit("dbs/SET_ADD_DB_COMP", "explorer");
      this.$store.state.console.partActive = "detail";
      this.$store.state.console.currentComponent = "DatabaseCreate";
    },

    collapseChange(data,node) {},

    // 这里由于有默认打开的key，所以其他同层已经打开的结构并不会触发收起回调
    async expandChange(data,node) {
      // 由于点击展开图标不会触发节点点击时间所以使用展开触发
      let result = [];
      if (node.expanded) {
        result.push(node.data["node-key"]);
      }
      let parentNode = node.parent;
      while (parentNode && parentNode.data) {
        result.push(node.data["node-key"]);
        result.push(parentNode.data["node-key"]);
        result = [...new Set(result)]
        parentNode = parentNode.parent;
      }
      this.defaultExpandedKeys = result.reverse();

      switch (data.typeName) {
        case "table":
          await this.$store
            .dispatch(
              "console/sendConsoleSQL",
              `select * from  \`${data.parent.split(".")[0]}\`` +
                "." +
                `\`${data.name}\`  order by _C0 desc limit 200`
            )
            .catch(() => false);
          break;
        case "database":
          this.$bus.emit("console/useDB", data.name);
          break;
        default:
          break;
      }
      this.changePartActive();
    },
    async loadNode(node, resolve) {
      let data = node.data;
      switch (node.data?.typeName) {
        case "sfile": //STables文件夹
          return resolve(
            ...(await getStableListReq(
              {
                pageSize: this.pageSize,
                currentPage: node.currentPage + 1,
              },
              data.parent
            ))
          );
        case "nfile": //Tables文件夹
          return resolve(
            ...(await getAllNormalTables(
              {
                pageSize: this.pageSize,
                currentPage: node.currentPage + 1,
              },
              data.parent
            ))
          );
        case "database":
          //databse下需要添加Stables文件夹和Tbales文件夹
          let result = [1, 2].map((item) => {
            return {
              columns: 2,
              create_time: moment(new Date().getTime()).format(
                "YYYY-MM-DD HH:mm:ss"
              ),
              db_name: node.data.name,
              last_update: moment(new Date().getTime()).format(
                "YYYY-MM-DD HH:mm:ss"
              ),
              max_delay: "",
              name: item === 1 ? "STables" : "Tables",
              "node-key": item === 1 ? "stable" : "ntable",
              parent: node.data.name,
              rollup: "",
              stable_name: "STables",
              table_comment: "25",
              tags: "",
              typeName: item == 1 ? "sfile" : "nfile",
              watermark: "0",
            };
          });
          return resolve(result);

        // return resolve(
        //   ...(await getStableListReq(
        //     {
        //       pageSize: this.pageSize,
        //       currentPage: node.currentPage + 1,
        //     },
        //     data.name
        //   ))
        // );

        case "stable":
          return resolve(
            ...(await getTableListReq({
              selected_stb: data.name,
              pageSize: this.pageSize,
              currentPage: node.currentPage + 1,
              selected_db: data.parent,
            }))
          );
        case "table":
          return resolve(
            await getMatrixStructReq({
              selected_db: data.parent.split(".")[0],
              selected_tb: data.name,
            })
          );
        default:
          // eslint-disable-next-line no-case-declarations
          let dbList = await getDBListReq();
          this.$store.commit("dbs/SET_DBLIST", dbList);
          return resolve(dbList);
      }
    },
    //只有在database时候处理
    setTreeFiles(node) {
      switch (node.typeName) {
        case "stable":
          (node["parent"] = "Stables"), (node["typeName"] = "sfile");
          return node;
        case "table":
          (node["parent"] = "Tables"), (node["typeName"] = "tfile");
          return node;
        default:
          return node;
      }
    },
    changePartActive() {
      if (clickNoChange.includes(this.$store.state.console.partActive)) {
        return;
      }
      this.$store.state.console.partActive = "sql";
    },
    // 处理全局db和stb
    async handleVar(data, node) {
      switch (data.typeName) {
        case "database":
          const name = data.name;
          Object.assign(data, await getDBStruct(data.name));
          data.name = name;
          this.$store.commit("dbs/SET_SELECTED_DB", data.name);
          this.$store.commit("stables/SET_SELECTED_STB", "");
          this.$store.commit("tables/SET_SELECTED_TB", "");
          break;
        case "sfile":
          this.$store.commit("dbs/SET_SELECTED_DB", node.parent.data.name);
          this.$store.commit("stables/SET_SELECTED_STB", node.parent.data.name);
          this.$store.commit("tables/SET_SELECTED_TB", "");
          break;

        case "nfile":
          this.$store.commit("dbs/SET_SELECTED_DB", data.parent);
          this.$store.commit("stables/SET_SELECTED_STB", "");
          break;
        case "stable":
          this.$store.commit("dbs/SET_SELECTED_DB", data.parent);
          this.$store.commit("stables/SET_SELECTED_STB", data.name);
          break;
        case "table":
          this.$store.commit("dbs/SET_SELECTED_DB", data.parent.split(".")[0]);
          this.$store.commit(
            "stables/SET_SELECTED_STB",
            data.parent.split(".")[1]
          );
          this.$store.commit("tables/SET_SELECTED_TB", data.name);
          break;
        default:
          break;
      }
      // if (!node) return;
      // let result = [];
      // // 判断当前节点是否为展开状态,后续父节点不需要判断
      // if (node.expanded) {
      //   result.push(node.data["node-key"]);
      // }
      // let currentNode = node.parent;
      // while (currentNode && currentNode.data) {
      //   result.push(currentNode.data["node-key"]);
      //   currentNode = currentNode.parent;
      // }
      // // 处理默认展开的key
      // this.defaultExpandedKeys = result.reverse();
    },
    async add(data, node) {
      await this.handleVar(data, node);
      switch (data.typeName) {
        case "database":
        case "sfile":
          this.$store.commit("dbs/HANDLE_EDIT_DB", deepClone(data));
          this.$store.commit("stables/HANDLE_ADD_STABLE");
          this.$store.state.console.currentComponent = "StableCreate";
          break;
        case "stable":
          await this.$store.dispatch("tables/handleUseStbCreate", data.name);
          this.$store.state.console.currentComponent = "TableCreate";
          break;
        case "table":
        case "nfile":
          this.$store.commit("tables/HANDLE_ADD_TABLE");
          this.$store.state.console.currentComponent = "TableCreate";
          break;

        default:
          break;
      }
      this.$store.commit("console/SET_TAB_NAME", "add");
      this.$store.state.console.partActive = "detail";
    },
    async view(data) {
      await this.handleVar(data);
      this.$store.state.console.currentInfoType = data.typeName === "table" ? data.type : data.typeName
      this.$store.commit("console/SET_CURRENT_INFO_DATA", data);
      this.$store.state.console.currentComponent = "Info";
      this.$store.commit(
        "console/SET_TAB_NAME",
          `console.${data.typeName === "table" ? data.type : data.typeName}Info`
      );
      this.$store.state.console.partActive = "detail";
    },
    async edit(data, node) {
      await this.handleVar(data, node);
      switch (data.typeName) {
        case "database":
          this.$store.commit("dbs/HANDLE_EDIT_DB", deepClone(data));
          this.$store.state.console.currentComponent = "DatabaseCreate";
          break;
        case "stable":
          this.$store.commit("dbs/HANDLE_EDIT_DB", deepClone(data));
          await this.$store.dispatch("stables/getStatleStruct", { stableName: data.name, type: 'create_stb'});
          this.$store.state.console.currentComponent = "StableCreate";
          break;
        case "table":
          await this.$store.dispatch("tables/getTableStruct", {
            tableName: data.name,
            stableName: data.stable_name,
          });
          this.$store.state.console.currentComponent = "TableCreate";
          break;

        default:
          break;
      }
      this.$store.commit("console/SET_TAB_NAME", "edit");
      this.$store.commit("dbs/SET_ADD_DB_COMP", "explorer");
      this.$store.state.console.partActive = "detail";
    },
    async del(data, node) {
      if (this.requesting) return;
      await this.handleVar(data, node);
      switch (data.typeName) {
        case "database":
          let task = [];
          if (!this.$COMMUNITY) {
            this.requesting = true;
            let result = await getRunningTask();
            task = result.filter((item) => item.to_expand?.subject == data.name);
          }
          if (task.length > 0) {
            this.$alert(
              this.$t("data.delRunningTaskBb")
                .replace("{dbName}", data.name)
                .replace("{taskName}", task[0]?.name),
              this.$t("tips"),
              {
                confirmButtonText: this.$t("confirm"),
                type: "warning",
                showClose: false
              }
            ).then(() => {
              this.requesting = false;
            });
          } else {
            this.$confirm(
              this.$t("data.delDatabase") + ":" + data.name + "?",
              this.$t("tips"),
              {
                confirmButtonText: this.$t("confirm"),
                cancelButtonText: this.$t("cancel"),
                type: "warning",
              }
            )
              .then(async () => {
                this.requesting = true;
                await this.$store
                  .dispatch("dbs/deleteDB", data.name)
                  .then(() => {
                    this.$message.success(this.$t("delSucc"));
                  })
                  .catch((err) => {
                    err.desc && this.$error(err.desc);
                  })
                  .finally(() => {
                    this.requesting = false;
                  })
                  .catch((res) => {
                    this.$error(res?.desc);
                  });
              })
              .catch(() => {
                this.requesting = false;
              });
          }
          break;
        case "stable":
          this.$confirm(
            `${this.$t("del")} ${this.$t("data.stable")} ${data.name} ?`,
            this.$t("tips"),
            {
              confirmButtonText: this.$t("confirm"),
              cancelButtonText: this.$t("cancel"),
              type: "warning",
            }
          ).then(async () => {
            this.requesting = true;
            await this.$store
              .dispatch("stables/deleteStable", {
                selected_db: data.parent,
                stableName: data.name,
              })
              .then(() => {
                this.$message.success(this.$t("delSucc"));
              })
              .catch((err) => {
                err.desc && this.$error(err.desc);
              })
              .finally(() => {
                this.requesting = false;
              })
              .catch((res) => {
                this.$error(res?.desc);
              });
          });
          break;
        case "table":
          this.$confirm(
            this.$t("del") +
              " " +
              data.name +
              " " +
              this.$t("data.table") +
              "?",
            this.$t("tips"),
            {
              confirmButtonText: this.$t("confirm"),
              cancelButtonText: this.$t("cancel"),
              type: "warning",
            }
          ).then(async () => {
            this.requesting = true;
            await this.$store
              .dispatch("tables/deleteTable", data.name)
              .then(() => {
                this.$message.success(this.$t("delSucc"));
              })
              .catch((err) => {
                err.desc && this.$error(err.desc);
              })
              .finally(() => {
                this.requesting = false;
              })
              .catch((res) => {
                this.$error(res?.desc);
              });
          });
          break;

        default:
          break;
      }
      this.changePartActive();
    },
    async manage(data, node) {
      await this.handleVar(data);
      this.$store.state.console.currentInfoType = data.typeName;
      this.$store.commit("console/SET_CURRENT_INFO_DATA", data);
      this.$store.state.console.currentComponent = "DatabasePrivileges";
      this.$store.commit(
        "console/SET_TAB_NAME",
        "data.databaseControl"
      );
      this.$store.commit(
        "console/SET_DB_NAME",
        data.name
      );
      this.$store.state.console.partActive = "detail";
    },
    async search(data, node) {
      this.searchdialog = true;
      this.currentSearch = data;
      switch (data.typeName) {
        case "sfile":
          this.showtag = false;
          this.switchtag = false;
          this.dialogtitle = this.$t("data.searchsp");
          break;
        case "stable":
          let result = await sendSQLReq(
            `describe ${data.db_name}.${data.name}`
          );
          this.tagList = result.data.map((db) => {
            return Object.fromEntries(
              result.column_meta.map((item, index) => {
                return [item[0], db[index]];
              })
            );
          });
          this.tagList = this.tagList
            .map((item) => {
              return Object.assign(
                item,
                {
                  conditionList: conditionMap["BOOL"],
                },
                {
                  value: "",
                },
                {
                  condition: "",
                },
                {
                  betweenVal: "",
                }
              );
            })
            .filter((val) => val.note == "TAG");

          this.showtag = true;
          this.dialogtitle = this.$t("data.searchsub");
          break;
        case "nfile":
          this.showtag = false;
          this.switchtag = false;
          this.dialogtitle = this.$t("data.searchnt");
          break;
      }
    },
    async searchTables() {
      try {
        let flag = true;
        this.$refs.searchForm.validate((valid) => {
          if (valid) {
            flag = true;
            return true;
          } else {
            flag = false;
            return false;
          }
        });
        if (!flag) {
          return;
        }
        switch (this.currentSearch.typeName) {
          case "sfile": //查询超级表
            await this.$store
              .dispatch(
                "console/sendConsoleSQL",
                `select stable_name from information_schema.ins_stables where db_name='${this.currentSearch.db_name}' and  stable_name like '%${this.serachForm.tablename}%'`
              )
              .catch(() => false);

            break;
          case "stable":
            if (this.switchtag) {
              for (let i = 0; i < this.tagList.length; i++) {
                if (
                  !this.tagList[i].condition ||
                  (!this.tagList[i].condition.includes("NULL") &&
                    !this.tagList[i].value)
                ) {
                  Message.warning(this.$t("data.fulltagtip"));
                  return;
                }
              }
              let wherestr = "";
              this.tagList.forEach((item, index) => {
                wherestr +=
                  " " +
                  (index == 0 ? "" : "and") +
                  " " +
                  `${item.field}` +
                  " " +
                  `${item.condition}` +
                  (item.condition.includes("NULL")
                    ? ""
                    : item.condition.includes("IN")
                    ? "(" + `'${item.value}'` + ")"
                    : " " + `'${item.value}'`);
                if (item.condition.includes("BETWEEN")) {
                  wherestr += ` AND '${item.betweenVal}'`;
                }
              });
              await this.$store
                .dispatch(
                  "console/sendConsoleSQL",
                  `select distinct tbname from \`${this.currentSearch.db_name}\`.\`${this.currentSearch.stable_name}\` where ${wherestr}`
                )
                .catch(() => false);
            } else {
              await this.$store
                .dispatch(
                  "console/sendConsoleSQL",
                  `select table_name from information_schema.ins_tables where db_name='${this.currentSearch.db_name}' and  stable_name='${this.currentSearch.stable_name}' and table_name like '%${this.serachForm.tablename}%'`
                )
                .catch(() => false);
            }

            break;
          case "nfile":
            await this.$store
              .dispatch(
                "console/sendConsoleSQL",
                `select table_name from information_schema.ins_tables where db_name='${this.currentSearch.db_name}' and  stable_name is null and table_name like '%${this.serachForm.tablename}%'`
              )
              .catch(() => false);
            break;
        }
        this.searchdialog = false;
      } catch (error) {
        console.log(error, "查询表格");
      }
    },
    getTooltip(data, operate) {
      let obj = {
        database: {
          add: this.$t("data.createStable", [data.name]),
          edit: this.$t("data.editDatabase"),
          view: this.$t("data.viewDatabase"),
          del: this.$t("data.delDatabase"),
          manage: this.$t("data.manageDBprivilege"),
        },
        sfile: {
          add: this.$t("data.createTableUse", [data.name]),
          edit: this.$t("data.editStable", [data.name]),
          view: this.$t("data.viewStable"),
          del: this.$t("data.delStable"),
          search: this.$t("data.searchsp"),
        },
        nfile: {
          add: this.$t("data.createnormalTable", [data.name]),
          edit: this.$t("data.editTable", [data.name]),
          view: this.$t("data.viewTable"),
          del: this.$t("data.delTable"),
          search: this.$t("data.searchnt"),
        },
        table: {
          add: this.$t("data.createnormalTable", [data.name]),
          edit: this.$t("data.editTable", [data.name]),
          view: this.$t("data.viewTable"),
          del: this.$t("data.delTable"),
        },
        stable: {
          add: this.$t("data.createsubTable", [data.name]),
          edit: this.$t("data.editTable", [data.name]),
          view: this.$t("data.viewTable"),
          del: this.$t("data.delTable"),
          search: this.$t("data.searchsub"),
        },
        column: {
          view: this.$t("data.viewTable"),
        },
        tag: {
          view: this.$t("data.viewTable"),
        },
      };
      return obj[data.typeName][operate];
    },
  },
};
</script>

<style lang="scss" scoped>
.dbs-tree {
  width: 100%;
  height: 100%;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}
.dbs-tree ::v-deep {
  .el-tree-node__content {
    height: 30px;
  }
  ::-webkit-scrollbar {
    width: 0px !important;
    height: 0px !important;
    background: rgba(255, 255, 255, 0);
  }
  ::-webkit-scrollbar-thumb {
    border-radius: 4px;
    background-color: rgba(220, 220, 220, 0);
  }
}
.database_icon {
  width: 18px;
  height: 18px;
  flex-shrink: 0;
}

.title {
  margin-left: 10px;
}
.dbs-tree-container {
  flex: 1;
  overflow: auto;
}
.table_wrapper {
  color: #333;
  display: flex;
  flex-direction: row;
  align-items: center;
  height: 30px;
  position: relative;
  .table_icon {
    width: 18px;
    height: 18px;
    flex-shrink: 0;
  }
  .tablename {
    margin-left: 10px;
  }
}
.tree-checkbox {
  margin-right: 10px;
}
.table_wrapper:hover:not(click) {
  cursor: pointer;
  color: #409eff;
  background-color: #ecf5ff;
}
.table_wrapper::before {
  cursor: pointer;
  color: #409eff;
  background-color: #ecf5ff;
}
.operate-btn {
  position: absolute;
  right: 0;
  opacity: 0;
  font-size: 12px;
  color: #fff;
  display: flex;
  align-items: center;
  background-color: $color-primary;
  height: 30px;
  padding-left: 10px;
  color: #fff;
}
.operate-icon {
  margin-right: 10px;
}
.tablebutton {
  cursor: pointer;
  display: flex;
  height: 100%;
  padding: 0px 0.5rem;
  -webkit-box-align: center;
  align-items: center;
  -webkit-box-pack: center;
  justify-content: center;
  // border: 1px solid rgb(40, 42, 54);
  outline: 0px;
  font-weight: 400;
  line-height: 1.15;
  // transition: all 70ms cubic-bezier(0, 0, 0.38, 0.9) 0s;
  background: inherit;
  color: rgb(248, 248, 242);
}
// .tablebutton:hover:not([disabled]) {
//   background: rgb(98, 114, 164);
//   color: rgb(248, 248, 242);
//   border-color: rgb(98, 114, 164);
// }
.filed::before {
  position: absolute;
  height: 100%;
  width: 2px;
  left: -1.2rem;
  top: 0px;
  content: "";
  background: rgb(88, 88, 88);
}

.filed_wrapper {
  color: #333;
  display: flex;
  flex-direction: row;
  align-items: center;
  height: 30px;
  // cursor: pointer;

  .table_icon {
    width: 18px;
    height: 18px;
    flex-shrink: 0;
  }
  .tablename {
    margin-left: 10px;
  }
}
.filed_wrapper:hover {
  color: #409eff;
  background-color: #ecf5ff;
}
</style>
<style lang="scss">
.tree-wrapper {
  position: relative;
  display: flex;
  justify-content: space-between;
  align-items: center;
  width: 100px;
  flex: 1;
  .column-type {
    color: #5961ff;
    font-style: italic;
    text-transform: lowercase;
    padding: 0 10px;
    flex-shrink: 0;
  }
  .console-tree-icon {
    width: 18px;
    height: 18px;
    margin-right: 10px;
    flex-shrink: 0;
  }
  .console-custom-tree-node {
    font-family: Menlo, Monaco, Consolas, "Liberation Mono", "Courier New",
      monospace;
    display: flex;
    align-items: center;
    line-height: 30px;
    @extend .nowrap;
  }
  &:hover {
    .operate-btn {
      opacity: 1;
    }
  }
}
.el-tree-popper {
  background: $color-primary !important;
}
.el-tooltip__popper.el-tree-popper[x-placement^="right"] .popper__arrow,
.el-tooltip__popper[x-placement^="right"] .popper__arrow::after {
  border-right-color: $color-primary !important;
}
.footer {
  display: flex;
  justify-content: center;
  .el-button {
    height: 36px;
    padding: 8px 20px;
    font-size: 14px;
  }
}
.tag-column {
  display: flex;
  .el-select {
    margin-right: 10px;
  }
}
.tag-list {
  .open-tag {
    display: flex;
    align-items: center;
    margin-bottom: 10px;
  }
  .label {
    color: #4d6992;
    font-size: 16px;
    display: block;
    font-weight: 500;
    margin-bottom: 15px;
    margin-right: 10px;
  }
}
</style>
