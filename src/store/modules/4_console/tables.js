import { deleteTableReq, getTagValue, createTableReq, getMatrixStructReq } from "@/api/gateway/data/tables";
import { getStableStructReq } from "@/api/gateway/data/stables";

const state = {
  tableList: [],
  currentPage: 1,
  pageSize: 10,
  total: 0,
  matcher: "",

  selected_tb: "",
  table_form: {},
  formStatus: "create",
};

const mutations = {
  SET_TABLE_LIST: (state, tableList) => {
    state.tableList = tableList;
  },
  SET_CURRENT_PAGE: (state, currentPage) => {
    state.currentPage = currentPage;
  },
  SET_PAGESIZE: (state, pageSize) => {
    state.pageSize = pageSize;
  },
  SET_TOTAL: (state, total) => {
    state.total = total;
  },
  SET_MATCHER: (state, matcher) => {
    state.matcher = matcher;
  },
  SET_SELECTED_TB: (state, selected_tb) => {
    state.selected_tb = selected_tb;
  },
  HANDLE_ADD_TABLE: (state, form,stbTmpl) => {
    if (form) {
      state.formStatus = "update";
    } else {
      state.formStatus = "create";
    }
    state.table_form = form || {
      name: "",
      stbTmpl: "",//创建普通表默认赋值
      ts_field_name: "",
      columns: [{type: "TIMESTAMP", field: "", value: "",varcharLength:8,ncharLength:8 },{ type: "INT", field: "", value: "",varcharLength:8,ncharLength:8 }],
    };
  },
  SET_TABLE_FORM: (state, table_form) => {
    state.table_form = table_form;
  },
};

const actions = {
  deleteTable({ commit, rootState }, tableName) {
    return deleteTableReq({
      selected_db: rootState.dbs.selected_db,
      tableName: tableName,
    }).finally(() => {
      commit("console/CHANGE_TREE_KEY", null, { root: true });
    });
  },
  handleUseStbCreate({ commit, rootState }, stableName) {
    commit("stables/SET_SELECTED_STB", stableName, { root: true });
    return getStableStructReq({
      selected_db: rootState.dbs.selected_db,
      stableName: stableName,
    }).then(res => {
      state.formStatus = "createByStb";
      let { ts_field_name, columns, tags } = res;
      commit("SET_TABLE_FORM", {
        name: "",
        stbTmpl: stableName,
        ts_field_name: ts_field_name,
        columns: columns,
        tags: tags,
      });
    });
  },
  getTableStruct({ state, commit, rootState }, payload) {
    return getMatrixStructReq({
      selected_db: rootState.dbs.selected_db,
      selected_tb: payload.tableName,
    })
      .then(async content => {
        state.formStatus = "update";
        let tags = [];
        let columns = [];
        content.forEach(item => {
          if (item.typeName == "tag") {
            tags.push({ field: item.name, type: item.dataType, value: "" });
          }
          if (item.typeName == "column") {
            columns.push({ field: item.name, type: item.dataType, value: "" });
          }
        });

        if (payload.stableName) {
          let tagValueObj = await getTagValue(tags, rootState.dbs.selected_db, payload.stableName, payload.tableName);
          tags.forEach(item => {
            item.value = tagValueObj[item.field];
          });
        }
        commit("HANDLE_ADD_TABLE", {
          name: payload.tableName,
          stbTmpl: payload.stableName || "",
          ts_field_name: content[0].name,
          columns,
          tags,
        });
      })
      .catch(() => {
        commit("HANDLE_ADD_TABLE", {});
      });
  },
  submitTableForm({ state, rootState, commit, dispatch }) {
    return createTableReq({
      selected_db: rootState.dbs.selected_db,
      table_form: state.table_form,
    })
      .then(() => {
        // dispatch("handleUseStbCreate", state.table_form.stbTmpl);
        commit("console/CHANGE_TREE_KEY", null, { root: true });
      })
      .catch((err) => {
        if (!state.table_form.columns?.length) {
          state.table_form.columns?.push({ type: "INT", field: "", value: "",varcharLength:8,ncharLength:8 });
        }
        if (!state.table_form.tags?.length) {
          state.table_form.tags?.push({ type: "INT", field: "", value: "",varcharLength:8,ncharLength:8 });
        }
        return Promise.reject(err);
      });
  },
};

export default {
  namespaced: true,
  state,
  mutations,
  actions,
};
