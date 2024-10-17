import { getUserList } from "@/api/user.js";
const state = {
  userList: [],
  pageSize: 10,
  currentPage: 1,
  total: 0,
};
const mutations = {
  SET_CURRENT_PAGE(state, currentPage) {
    state.currentPage = currentPage;
  },
};
const actions = {
  getUserList({ state, commit }, currentPage = 1) {
    commit("SET_CURRENT_PAGE", currentPage);
    return getUserList({
      current_page: state.currentPage,
      page_size: state.pageSize,
    })
      .then(({ content, total }) => {
        state.userList = content;
        state.total = +total;
      })
      .catch(() => {
        state.userList = [];
        state.total = 0;
      });
  },
};
export default {
  namespaced: true,
  state,
  mutations,
  actions,
};
