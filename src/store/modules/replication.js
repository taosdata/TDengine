import { getDBListReq } from "api/gateway/data/dbs";
import { getTaskList } from "@/api/replication";
const state = {
  dbList: [],
  taskList: [],
};
const mutations = {
  SET_DB_LIST(state, dbList) {
    state.dbList = dbList;
  },
};
const actions = {
  getTaskList({ rootGetters }) {
    return getTaskList(rootGetters.appId)
      .then(data => {
        state.taskList = handleTaskList(data);
      })
      .catch(() => {
        state.taskList = [];
      });
  },
  getDBList() {
    return getDBListReq()
      .then(data => {
        state.dbList = data;
      })
      .catch(() => (state.dbList = []));
  },
};
function handleTaskList(taskList) {
  return taskList.map(item => {
    const { from } = item;
    item.fromToken = from.match(/\?token=([^&]+)/)?.[1];
    const groupId = from.match(/group.id=([^&]+)/)?.[1];
    item.fromDb = groupId.match(new RegExp(`${item.from_cluster}_(.+)_to`))?.[1];
    item.toDb = groupId.match(new RegExp(`${item.to_cluster}_(.+)`))[1];
    item.toToken = item.to.match(/token=([^&]+)/)?.[1];
    return item;
  });
}
export default {
  namespaced: true,
  state,
  mutations,
  actions,
};
