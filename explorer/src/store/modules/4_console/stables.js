import { getStableListReq, deleteStableReq, createStableReq, getStableStructReq } from "@/api/gateway/data/stables";

const state = {
  stableList: [],
  currentPage: 1,
  pageSize: 8,
  total: 0,
  matcher: "",
  tagDuplicate: [],
  selected_stb: "",
  stable_form: {},
  formStatus: "create",
};

const mutations = {
  SET_STABLE_LIST: (state, stableList) => {
    state.stableList = stableList;
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
  SET_SELECTED_STB: (state, selected_stb) => {
    state.selected_stb = selected_stb;
  },
  HANDLE_ADD_STABLE: (state, form) => {
    state.formStatus = form ? "update" : "create";
    state.stable_form = form || {
      name: "",
      rollup: "",
      columns: [
        { type: "TIMESTAMP", field: "", value: "",
          encode: "delta-i", 
          compress: "lz4", level: "medium", primaryKey: false,
          length: 8 
        },
        {
          type: "INT", field: "", value: "",
          encode: "simple8b", compress: "lz4", level: "medium",
          primaryKey: false, length: 8 
        }
      ],
      tags: [{
        type: "INT", field: "", value: "",
        length: 8
      }],
      ts_field_name: "",
    };
  },
};

const actions = {
  getStableList({ state, commit, rootState }, params = { current_page: 1 }) {
    commit("SET_CURRENT_PAGE", params.current_page || 1);
    let defaultParams = { current_page: 1, page_size: state.pageSize };
    if (state.matcher) {
      defaultParams.name = state.matcher;
    }
    return getStableListReq(
      {
        ...defaultParams,
        ...params,
      },
      rootState.dbs.selected_db
    )
      .then(res => {
        let { total, data } = res;
        commit("SET_STABLE_LIST", data);
        commit("SET_TOTAL", total);
      })
      .catch(() => {
        commit("SET_STABLE_LIST", []);
        commit("SET_TOTAL", 0);
      });
  },
  deleteStable({ commit }, payload) {
    let { selected_db, stableName } = payload;
    return deleteStableReq({
      selected_db: selected_db,
      stableName: stableName,
    }).finally(() => {
      commit("console/CHANGE_TREE_KEY", null, { root: true });
    });
  },
  getStatleStruct({ commit, rootState }, payload) {
    let { stableName, type } = payload;
    return getStableStructReq({
      selected_db: rootState.dbs.selected_db,
      stableName: stableName,
      type: type
    }).then(res => {
      let { ts_field_name, columns, tags } = res;
      state.tagDuplicate = JSON.parse(JSON.stringify(tags));
      commit("HANDLE_ADD_STABLE", {
        name: stableName,
        ts_field_name: ts_field_name,
        columns: columns,
        tags: tags,
      });
    });
  },
  submitStableForm({ state, commit }, selected_db) {
    return createStableReq({
      selected_db: selected_db,
      stable_form: state.stable_form,
    })
      .then(() => {
        commit("HANDLE_ADD_STABLE");
        commit("console/CHANGE_TREE_KEY", null, { root: true });
      })
      .catch((err) => {
        if (!state.stable_form.columns.length) {
          state.stable_form.columns.push({ type: "INT", field: "", value: "", length: 8,encode: "simple8b", compress: "lz4", level: "medium", primaryKey: false});
        }
        if (!state.stable_form.tags.length) {
          state.stable_form.tags.push({ type: "INT", field: "", value: "", length: 8 });
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
