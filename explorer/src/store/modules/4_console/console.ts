import { sendSQLReq, getFavorites } from '@/api/explorer';
import { ElMessage } from 'element-plus';
const state = {
  sqlStr: '',
  history: [],
  useCluster: '',
  useDB: '',
  selected_record: {},
  activeTab: 'grid',
  outputs: [],
  favorites: [],
  sharedFavorites: [],
  result: [],
  head: [],
  addSql: '',
  partActive: 'sql',
  currentComponent: '',
  treeKey: 0,
  currentInfoType: 'database',
  currentInfoData: {},
  tabName: '',
  currentOutput: {},
  shellData: [],
  fields: [],
  previewBtn: true,
  repeatResult: [],
  sharedTotal: 0,
  total: 0,
  selectedSqlStr: '',
  gridLoading: false,
  dbName: ''
};

const mutations = {
  SET_REPEATRESULT: (state, data) => {
    state.repeatResult = data;
  },
  SET_SQLSTR: (state, sqlStr) => {
    state.sqlStr = sqlStr;
  },
  ADD_SQLSTR: (state, sqlStr) => {
    state.sqlStr += sqlStr;
  },
  ADD_RECORD: (state, record) => {
    // const { appId } = record;
    state.currentOutput = record;
    state.history.push(record);
    state.history = state.history.slice(-100);
    localStorage.setItem('record_history', JSON.stringify(state.history));
  },
  SET_USE_DB: (state, useDB) => {
    state.useDB = useDB;
    // Cookies.set('useDB', useDB)
  },
  SET_SELECTED_RECORD: (state, payload) => {
    state.selected_record = {
      rawSQL: payload.rawSQL,
      parsedSQL: payload.parsedSQL
    };
  },
  SET_TAB_NAME(state, name) {
    state.tabName = name;
  },
  SET_DB_NAME(state, name) {
    state.dbName = name;
  },
  SET_ACTIVE_TAB: (state, activeTab) => {
    state.activeTab = activeTab;
  },
  CHANGE_TREE_KEY(state) {
    state.treeKey += 1;
  },
  CANCEL_DETAIL(state) {
    state.tabName = '';
    state.partActive = 'sql';
  },
  SET_CURRENT_INFO_DATA(state, data) {
    state.currentInfoData = data;
  },
  SET_FAVORITE(state, data) {
    state.favorites = data;
  },
  SET_SHAREDFAVOURTIE(state, data) {
    state.sharedFavorites = data;
  },
  SET_FIELEDS(state, data) {
    state.fields = data;
  },
  SET_PREVIEW_BTN(state, data) {
    state.previewBtn = data;
  },
  RESET_GRID(state) {
    state.head = [];
    state.result = [];
  }
};

const actions = {
  sendConsoleSQL({ state, commit, rootState }, sql) {
    if (!sql) return;
    const startTime = Date.now();
    state.gridLoading = true;
    return sendSQLReq(sql)
      .then(res => {
        handleSuccess(res, state, commit, rootState, sql, startTime);
        updateTree(res, commit);
        state.gridLoading = false;
      })
      .catch(res => {
        handleFail(res, state, commit, rootState, sql, startTime);
        state.gridLoading = false;
      });
  },
  getFavorites({ state }, params) {
    return getFavorites(params)
      .then(res => {
        state.favorites = res.data.list;
        state.total = res.data.total;
      })
      .catch(err => console.log('error', err));
  },
  getSharedFavorites({ state }, params) {
    return getFavorites(params)
      .then(res => {
        state.sharedFavorites = res.data.list;
        state.sharedTotal = res.data.total;
      })
      .catch(() => err => console.log('error', err));
  }
};

function updateTree(res, commit) {
  // 有 affected_rows 这个字段，并且等于0，才去刷新
  const metaIndex = res.column_meta[0].findIndex(item => item == 'affected_rows');
  if (metaIndex != -1 && res.data[0][metaIndex] == 0) {
    commit('CHANGE_TREE_KEY');
  }
}
function handleSuccess(res, state, commit, rootState, sql, startTime) {
  // 记录执行成功历史
  const data = res.data.map(item => item.map(val => val + ''));
  const head = res.column_meta.map(item => item[0]);
  commit('ADD_RECORD', {
    createdAt: Date.now(),
    time: Date.now() - startTime,
    cluster: rootState.app.current_cluster.name,
    database: state.useDB,
    sql: sql,
    type: 1,
    rows: res.rows,
    message: 'success',
    appId: rootState.app.current_cluster?.id
  });
  // 切换到output panel显示执行结果
  commit('SET_ACTIVE_TAB', 'grid');
  state.shellData = [head.map(item => item)].concat(data);
  state.result = Object.freeze(data);
  commit('SET_REPEATRESULT', data);
  state.head = Object.freeze(head);
}
function handleFail(res, state, commit, rootState, sql, startTime) {
  ElMessage.closeAll();
  const message = extractErrorMessage(res) || 'Unexpected error';
  if (message) {
    ElMessage.error({
      message,
      duration: 15000,
      showClose: true
    });
  }
  // status != "succ" 记录执行失败历史
  commit('ADD_RECORD', {
    createdAt: Date.now(),
    time: Date.now() - startTime,
    cluster: rootState.app.current_cluster.name,
    database: state.useDB,
    sql: sql,
    type: 0,
    rows: 0,
    message: message,
    appId: rootState.current_cluster?.id
  });
  commit('SET_ACTIVE_TAB', 'log');
}

function extractErrorMessage(err: any): string {
  try {
    if (!err) return '';
    if (typeof err === 'string') return err;
    if (typeof err?.desc === 'string') return err.desc;
    if (typeof err?.message === 'string' && !err?.response) return err.message;

    const data = err?.response?.data;
    if (typeof data === 'string') return data;
    if (typeof data?.desc === 'string') return data.desc;
    if (typeof data?.message === 'string') return data.message;
    if (data && typeof data === 'object') {
      if (typeof data.code !== 'undefined' && typeof data.desc === 'string') return data.desc;
      return JSON.stringify(data);
    }
    return err?.toString?.() || '';
  } catch (_e) {
    return '';
  }
}
export default {
  namespaced: true,
  state,
  mutations,
  actions
};
