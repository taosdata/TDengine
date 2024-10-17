import { getDBListReq, createDB, deleteDBReq, updateDB } from "@/api/gateway/data/dbs";
import { DBFILED } from "@/const";
const dbDefaultField = Object.fromEntries(Object.keys(DBFILED).map(key => [key, DBFILED[key].defaultValue]));
const state = {
  dbList: [],
  currentPage: 1,
  pageSize: 10,
  total: 0,
  matcher: "",
  selected_db: "",

  //DatabaseDialog
  dialogFormVisible: false,
  db_form: {},
  formStatus: "create",
  curComp: 'explorer',
  dialogDbVisible: false,
  currentdbName:''
};
// 修改数据库前的值
let dbConfigTemp = {};
const mutations = {
  SET_CURRENT_DBNAME:(state,data)=>{
    state.currentdbName=data
  },
  SET_DBLIST: (state, dbList) => {
    state.dbList = dbList;
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
  HANDLE_ADD_DB: state => {
    state.formStatus = "create";
    state.db_form = {
      name: "",
      ...dbDefaultField,
    };
  },
  HANDLE_EDIT_DB: (state, db_form) => {
    state.formStatus = "update";
    // TODO 3.0bug暂时兼容处理
    db_form.retentions = db_form.retentions || db_form.retention;
    state.db_form = db_form;
    dbConfigTemp = { ...db_form };
  },

  HANDLE_CLOSE_DIALOG: state => {
    state.dialogFormVisible = false;
  },
  SET_SELECTED_DB: (state, selected_db) => {
    state.selected_db = selected_db;
  },
  SET_ADD_DB_COMP: (state, curComp) => {
    state.curComp = curComp
  },
  SET_DIALOG_DB_VISABLE: (stat, dialogDbVisible) => {
    state.dialogDbVisible = dialogDbVisible
  }
};

const actions = {
  getDBList({ state, commit }, params = { current_page: 1 }) {
    commit("SET_CURRENT_PAGE", params.current_page || 1);
    let defaultParams = { current_page: 1, page_size: state.pageSize };
    if (state.matcher) {
      defaultParams.name = state.matcher;
    }
    return getDBListReq({ ...defaultParams, ...params })
      .then(res => {
        let { total, data } = res;
        commit("SET_DBLIST", data);
        commit("SET_TOTAL", total);
      })
      .catch(() => {
        commit("SET_DBLIST", []);
        commit("SET_TOTAL", 0);
      });
  },
  deleteDB({ commit }, dbName) {
    return deleteDBReq({
      dbName: dbName,
    }).finally(() => {
      commit("console/CHANGE_TREE_KEY", null, { root: true });
    });
  },
   createDatabase({ state, commit }) {
    return new Promise((resolve, reject) => {
      let execFn = createDB;
      let params = state.db_form;
      let name = state.db_form.name;
      if (state.formStatus == "update") {
        execFn = updateDB;
        params = {};
        for (let k in state.db_form) {
          if (state.db_form[k] != dbConfigTemp[k]) {
            params[k] = state.db_form[k];
          }
        }
      }
      if (JSON.stringify(params) === '{}') return resolve();
      execFn(params, name)
        .then(() => {
          // 解决删除后又立马创建同名数据库
          commit('SET_CURRENT_DBNAME',"")
          commit('SET_CURRENT_DBNAME',name)
          if (state.formStatus == "create") {
            commit("HANDLE_ADD_DB");
          }
          dbConfigTemp = { ...state.db_form };
          commit("console/CHANGE_TREE_KEY", null, { root: true });
          resolve()
        })
        .catch(err => {
          reject(err)
        });

    })
  },
  editDatabase() {
  },
};

export default {
  namespaced: true,
  state,
  mutations,
  actions,
};
