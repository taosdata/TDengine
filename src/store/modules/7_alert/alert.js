import { getAlertList } from "@/api/gateway/alert";

const state = {
  alertList: [],
  currentPage: 1,
  pageSize: 10,
  total: 0,
  alertId: "", //当有值时表示自动弹出详情
};

const mutations = {
  SET_ALERTLIST: (state, alertList) => {
    state.alertList = alertList;
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

const actions = {
  getAlertList({ commit, state }, params = { current_page: 1 }) {
    commit("SET_CURRENT_PAGE", params.current_page || 1);
    getAlertList({
      current_page: 1,
      page_size: state.pageSize,
      sort: "DESC",
      ...params,
    })
      .then(res => {
        let { total, content } = res;
        commit("SET_ALERTLIST", content);
        commit("SET_TOTAL", Number(total));
      })
      .catch(() => {
        commit("SET_ALERTLIST", []);
        commit("SET_TOTAL", 0);
      });
  },
};

export default {
  namespaced: true,
  state,
  mutations,
  actions,
};
