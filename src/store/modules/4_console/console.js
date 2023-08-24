import { sendSQLReq, getFavorites, getSharedFavorites } from "@/api/gateway/console";
import { compHeadAndData } from "@/utils";
import { Message } from "element-ui";
const state = {
  sqlStr: "",
  history: [],
  useCluster: "",
  useDB: "",
  selected_record: {},
  activeTab: "grid",
  outputs: [],
  favorites: [],
  sharedFavorites: [],
  result: [],
  head: [],
  addSql: "",
  partActive: "wizard",
  currentComponent: "",
  treeKey: 0,
  currentInfoType: "database",
  currentInfoData: {},
  tabName: "",
  currentOutput: {},
  shellData: [],
  fields:[],
  previewBtn: true
};

const mutations = {
  SET_SQLSTR: (state, sqlStr) => {
    state.sqlStr = sqlStr;
  },
  ADD_SQLSTR: (state, sqlStr) => {
    state.sqlStr += sqlStr;
  },
  ADD_RECORD: (state, record) => {
    const { appId } = record;
    state.currentOutput = record;
    state.history.push(record);
    state.history = state.history.slice(-100);
    localStorage.setItem("record_history" + appId, JSON.stringify(state.history));
  },
  SET_USE_DB: (state, useDB) => {
    state.useDB = useDB;
    // Cookies.set('useDB', useDB)
  },
  SET_SELECTED_RECORD: (state, payload) => {
    state.selected_record = {
      rawSQL: payload.rawSQL,
      parsedSQL: payload.parsedSQL,
    };
  },
  SET_TAB_NAME(state, name) {
    state.tabName = name;
  },
  SET_ACTIVE_TAB: (state, activeTab) => {
    state.activeTab = activeTab;
  },
  CHANGE_TREE_KEY(state) {
    state.treeKey += 1;
  },
  CANCEL_DETAIL(state) {
    state.tabName = "";
    state.partActive = "sql";
  },
  SET_CURRENT_INFO_DATA(state, data) {
    state.currentInfoData = data;
  },
  SET_FAVORITE(state,data){
    state.favorites=data
  },
  SET_SHAREDFAVOURTIE(state,data){
    state.sharedFavorites=data
  },
  SET_FIELEDS(state,data){
    state.fields=data
  },
  SET_PREVIEW_BTN(state,data){
    state.previewBtn = data
  }
};

const actions = {
  sendConsoleSQL({ state, commit, rootState }, sql) {
    if (!sql) return;
    let startTime = Date.now();
    return sendSQLReq(sql)
      .then(res => {
        handleSuccess(res, state, commit, rootState, sql, startTime);
      })
      .catch(res => {
        handleFail(res, state, commit, rootState, sql, startTime);
      });
  },
  // getFavorites({ state }) {
  //   return getFavorites()
  //     .then(res => {
  //       state.favorites = res;
  //     })
  //     .catch(() => (state.favorites = []));
  // },
  getSharedFavorites({ state }) {
    return getSharedFavorites()
      .then(data => {
        state.sharedFavorites = data;
      })
      .catch(() => (state.sharedFavorites = []));
  },
};
function handleSuccess(res, state, commit, rootState, sql, startTime) {
  // 记录执行成功历史
  let data = res.data.map(item => item.map(val => val + ""));
  let head = res.column_meta.map(item => item[0]);
  commit("ADD_RECORD", {
    createdAt: Date.now(),
    time: Date.now() - startTime,
    cluster: rootState.app.current_cluster.name,
    database: state.useDB,
    sql: sql,
    type: 1,
    rows: res.rows,
    message: "success",
    appId: rootState.app.current_cluster?.id,
  });
  // 切换到output panel显示执行结果
  commit("SET_ACTIVE_TAB", "grid");
  state.shellData = [head.map(item => item)].concat(data);
  state.result = Object.freeze(compHeadAndData(res.column_meta, data));
  state.head = Object.freeze(head);
}
function handleFail(res, state, commit, rootState, sql, startTime) {
  Message.closeAll();
  res.desc &&
    Message.error({
      message: res.desc,
      duration: 15000,
      showClose: true,
    });
  // status != "succ" 记录执行失败历史
  commit("ADD_RECORD", {
    createdAt: Date.now(),
    time: Date.now() - startTime,
    cluster: rootState.app.current_cluster.name,
    database: state.useDB,
    sql: sql,
    type: 0,
    rows: 0,
    message: res.response&&res.response.status==500?res.response.data:res.desc,
    appId: rootState.current_cluster?.id,
  });
  commit("SET_ACTIVE_TAB", "log");
}
export default {
  namespaced: true,
  state,
  mutations,
  actions,
};
