import { getActivityListReq } from "@/api/gateway/activity";

const state = {
  activityList: [],
  currentPage: 1,
  pageSize: 10,
  total: 0,
};

const mutations = {
  SET_ACTIVITYLIST: (state, activityList) => {
    state.activityList = activityList;
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
  getActivityList({ commit, state }, params) {
    commit("SET_CURRENT_PAGE", params.current_page);
    getActivityListReq({
      page_size: state.pageSize,
      ...params,
    })
      .then(res => {
        let { total, data } = res;
        commit("SET_ACTIVITYLIST", data);
        commit("SET_TOTAL", total);
      })
      .catch(() => {
        commit("SET_ACTIVITYLIST", []);
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
