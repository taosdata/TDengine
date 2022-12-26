import Vue from "vue";
import Vuex from "vuex";
import getters from "./getters";

Vue.use(Vuex);

const modulesFiles = require.context("./modules", true, /\.js$/);

// auto require all vuex module from modules file
const modules = modulesFiles.keys().reduce((modules, modulePath) => {
  const moduleName = modulePath
    .replace(/^\.\/(.*)\.\w+$/, "$1")
    .split("/")
    .pop();
  const value = modulesFiles(modulePath);
  modules[moduleName] = value.default;
  return modules;
}, {});

const store = new Vuex.Store({
  modules,
  getters,
  state: {
    contactDialogVisible: false, //联系客服弹窗
    upgradeDialogVisible: false, //升级弹窗
    language: process.env.VUE_APP_LANGUAGE || "en",
  },
  mutations: {
    SET_CONTACT_DIALOG_VISIBLE(state, visible) {
      state.contactDialogVisible = visible;
    },
    SET_UPGRADE_DIALOG_VISIBLE(state, visible) {
      state.upgradeDialogVisible = visible;
    },
    SET_LANGUAGE: (state, language) => {
      state.language = language;
    },
  },
});
export default store;
