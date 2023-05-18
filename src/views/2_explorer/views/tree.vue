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
        @node-collapse="expandChange"
        @node-expand="expandChange"
      >
        <el-tooltip
          slot-scope="{ node, data }"
          effect="dark"
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
                  :content="getTooltip(data, 'view')"
                  v-if="!['sfile', 'nfile','column','tag'].includes(data.typeName)"
                >
                  <i
                    class="el-icon-view operate-icon"
                    @click.stop="view(data)"
                  ></i>
                </el-tooltip>
                <template v-if="!data.noOperate">
                  <el-tooltip
                    effect="light"
                    placement="top"
                    :content="getTooltip(data, 'add')"
                    v-if="['sfile', 'nfile','stable'].includes(data.typeName)"
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
                    :content="getTooltip(data, 'edit')"
                    v-if="!['sfile', 'nfile','column','tag'].includes(data.typeName)"
                  >
                    <i
                      v-permission
                      class="el-icon-edit operate-icon"
                      @click.stop="edit(data, node)"
                      v-if="!['sfile', 'nfile','column','tag'].includes(data.typeName)"
                    ></i>
                  </el-tooltip>
                  <template v-if="isRoot === 'root'">
                    <el-tooltip
                      effect="light"
                      placement="top"
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
                    :content="getTooltip(data, 'del')"
                    v-if="!['sfile', 'nfile','column','tag'].includes(data.typeName)"
                  >
                    <i
                      v-permission
                      class="el-icon-delete operate-icon"
                      @click.stop="del(data, node)"
                      v-if="!['sfile', 'nfile','column','tag'].includes(data.typeName)"
                    ></i>
                  </el-tooltip>
                </template>
              </template>
              <el-tooltip
                v-if="data.typeName == 'table' || data.typeName == 'stable'"
                effect="light"
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
              <el-tooltip effect="light" :content="$t('data.appendEditor')">
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
import PanelHeader from "./components/panelHeader.vue";
import VueEasyTree from "@/components/Tree";
import { deepClone } from "@/utils";
import moment from "moment";
import { Message } from "element-ui";
const clickNoChange = ["sql", "xterm"];
export default {
  props: {
    addSql: {
      type: String,
      default: "",
    },
  },
  components: { PanelHeader, VueEasyTree },
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
          }).catch(() => ({
            ts_field_name: "",
            columns: [],
            tags: [],
          }));
          columns = [`\`${sdata.ts_field_name}\``].concat(
            sdata.columns.map((item) => `\`${item.field}\``)
          );
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
      this.$store.commit("console/SET_TAB_NAME", this.$t("add"));
      this.$store.state.console.partActive = "detail";
      this.$store.state.console.currentComponent = "DatabaseCreate";
    },

    // 这里由于有默认打开的key，所以其他同层已经打开的结构并不会触发收起回调
    async expandChange(data) {
      // 由于点击展开图标不会触发节点点击时间所以使用展开触发
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
              "node-key": item === 1 ? "stable" : "ntable" + Math.random(),
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
          //操作数据库时，获取数据库配置
          // if (!Object.prototype.hasOwnProperty.call(data, "minrows")) {
          //   console.log(await getDBStruct(data.parent),'为啥assign-----');
          //   Object.assign(data, await getDBStruct(data.name));
          // }
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
      if (!node) return;
      let result = [];
      // 判断当前节点是否为展开状态,后续父节点不需要判断
      if (node.expanded) {
        result.push(node.data["node-key"]);
      }
      let currentNode = node.parent;
      while (currentNode && currentNode.data) {
        result.push(currentNode.data["node-key"]);
        currentNode = currentNode.parent;
      }
      // 处理默认展开的key
      this.defaultExpandedKeys = result.reverse();
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
      this.$store.commit("console/SET_TAB_NAME", this.$t("add"));
      this.$store.state.console.partActive = "detail";
    },
    async view(data) {
      await this.handleVar(data);
      this.$store.state.console.currentInfoType = data.typeName;
      this.$store.commit("console/SET_CURRENT_INFO_DATA", data);
      this.$store.state.console.currentComponent = "Info";
      this.$store.commit(
        "console/SET_TAB_NAME",
        this.$t(`console.${data.typeName}Info`)
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
          await this.$store.dispatch("stables/getStatleStruct", data.name);
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
      this.$store.commit("console/SET_TAB_NAME", this.$t("edit"));
      this.$store.state.console.partActive = "detail";
    },
    async del(data, node) {
      if (this.requesting) return;
      await this.handleVar(data, node);
      switch (data.typeName) {
        case "database":
          this.$confirm(
            this.$t("data.delDatabase") + ":" + data.name + "?",
            this.$t("tips"),
            {
              confirmButtonText: this.$t("confirm"),
              cancelButtonText: this.$t("cancel"),
              type: "warning",
            }
          ).then(async () => {
            this.requesting = true;
            await this.$store
              .dispatch("dbs/deleteDB", data.name)
              .then(() => {
                this.$message.success(this.$t("delSucc"));
              })
              .catch((err) => {
                err.desc && Message.error(err.desc);
              })
              .finally(() => {
                this.requesting = false;
              });
          });
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
                err.desc && Message.error(err.desc);
              })
              .finally(() => {
                this.requesting = false;
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
                err.desc && Message.error(err.desc);
              })
              .finally(() => {
                this.requesting = false;
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
        this.$t(`data.databaseControl`).replace("{dbName}", data.name)
      );
      this.$store.state.console.partActive = "detail";
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
        },
        nfile: {
          add: this.$t("data.createnormalTable", [data.name]),
          edit: this.$t("data.editTable", [data.name]),
          view: this.$t("data.viewTable"),
          del: this.$t("data.delTable"),
        },
        table: {
          add: this.$t("data.createnormalTable", [data.name]),
          edit: this.$t("data.editTable", [data.name]),
          view: this.$t("data.viewTable"),
          del: this.$t("data.delTable"),
        },
        stable: {
          add: this.$t("data.createnormalTable", [data.name]),
          edit: this.$t("data.editTable", [data.name]),
          view: this.$t("data.viewTable"),
          del: this.$t("data.delTable"),
        },
        column:{
          view:this.$t("data.viewTable"),
        },
        tag:{
          view:this.$t("data.viewTable"),
        }
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
.dbs-tree ::v-deep .el-tree-node__content {
  height: 30px;
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
  overflow: hidden;
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
</style>
