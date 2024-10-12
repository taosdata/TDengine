import { getIssueListReq, createNewIssueReq, getIssueTypeListReq } from "@/api/gateway/support";
import router from "@/router";

const state = {
  issueList: [],
  currentPage: 1,
  pageSize: 10,
  total: 0,
  filter: "",
  issuetype_list: [],
  dialogFormVisible: false,
  issue_form: {},
  current_issuetype: {},
};

const mutations = {
  SET_FILTER(state, filter) {
    state.filter = filter;
  },
  // IssuesTable
  SET_ISSUELIST: (state, issueList) => {
    state.issueList = issueList;
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

  // NewIssuesDialog
  HANDLE_ADD_ISSUE_BY_TYPE: (state, issueType) => {
    state.dialogFormVisible = false;
    state.current_issuetype = issueType;
  },
  SET_ISSUE_TYPE_LIST: (state, issuetype_list) => {
    state.issuetype_list = issuetype_list;
  },
  HANDLE_OPEN_DIALOG: state => {
    state.dialogFormVisible = true;
  },
  HANDLE_CLOSE_DIALOG: state => {
    state.dialogFormVisible = false;
  },
  SET_CURRENT_ISSUE_TYPE: (state, id) => {
    let type = state.issueTypes.find(type => {
      return type.id == id;
    });
    state.current_issuetype = type;
  },
};

const actions = {
  getIssueList({ commit, state }, params = { current_page: 1 }) {
    commit("SET_CURRENT_PAGE", params.current_page || 1);
    getIssueListReq({
      current_page: state.currentPage,
      page_size: state.pageSize,
      title: state.filter,
      ...params,
    })
      .then(res => {
        let { total, data } = res;
        commit("SET_ISSUELIST", data);
        commit("SET_TOTAL", total);
      })
      .catch(() => {
        commit("SET_ISSUELIST", []);
        commit("SET_TOTAL", 0);
      });
  },
  getIssueTypeList({ commit }) {
    getIssueTypeListReq()
      .then(issuetype_list => {
        commit("SET_ISSUE_TYPE_LIST", issuetype_list);
      })
      .catch(() => {
        commit("SET_ISSUE_TYPE_LIST", []);
      });
  },
  createNewIssue({ state, dispatch }) {
    createNewIssueReq({
      title: state.issue_form.title,
      description: state.issue_form.desc,
      type: state.current_issuetype.id,
    }).then(() => {
      router.push("/support/issues");
      dispatch("getIssueList", 1);
    });
  },
};

export default {
  namespaced: true,
  state,
  mutations,
  actions,
};
