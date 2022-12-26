import { getSlowSqlListReq } from "@/api/gateway/sql/slow_sql";
const state = {
  slowList: [],
  currentPage: 1,
  pageSize: 10,
  total: 0,
};

const mutations = {
  SET_SLOW_LIST: (state, slowList) => {
    state.slowList = slowList;
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
};
// let defaultParams = {
//   low: -1,
//   high: -1,
// };
const actions = {
  getSlowSqlList({ state, commit, rootState }, params = 1) {
    params = handleListParams(params, commit, state, rootState);
    getSlowSqlListReq(params)
      .then(res => {
        let { data, total } = res;
        commit("SET_SLOW_LIST", data);
        commit("SET_TOTAL", total);
      })
      .catch(() => {
        commit("SET_SLOW_LIST", []);
        commit("SET_TOTAL", 0);
      });
  },
};

// 处理获取列表数据参数
function handleListParams(params, commit, state, rootState) {
  if (typeof params == "number") {
    params = {
      current_page: params,
    };
  }
  // 参数中有pagesize就设置一下
  if (params.page_size) {
    commit("SET_PAGESIZE", params.page_size);
  } else {
    params.page_size = state.pageSize;
  }
  params.app_id = rootState.app.current_cluster?.id;
  commit("SET_CURRENT_PAGE", params.current_page);
  return params;
}
export default {
  namespaced: true,
  state,
  mutations,
  actions,
};
