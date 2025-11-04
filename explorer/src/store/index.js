// import Vue from "vue";
// import Vuex from "vuex";
import getters from "./getters";


import { createStore } from 'vuex'

// Vue.use(Vuex);

const modulesFiles = import.meta.glob('./modules/*.ts', { eager: true });

// auto require all vuex module from modules file
const modules = {};
for (const path in modulesFiles) {
  const moduleName = path.replace(/.+\/([^/.]+)\.\w+/, '$1');
  modules[moduleName] = modulesFiles[path].default;
  if (import.meta.hot) {
    // import.meta.hot.accept(acceptHMRUpdate(modules[moduleName], import.meta.hot));
  }
}

const store = createStore({
  modules,
  getters,
  state: {
    contactDialogVisible: false, //联系客服弹窗
    upgradeDialogVisible: false, //升级弹窗
    language: import.meta.env.VUE_APP_LANGUAGE || "en",
    dialogVisible: false, //弹窗
    dialogConfig: {}, //弹窗配置
    dialogParams: {}, //弹窗参数
    dialogListeners: {}, //弹窗监听
    dialogComponent: null, //弹窗组件
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
    SET_DIALOG_VISIBLE(state, visible) {
      state.dialogVisible = visible;
    },
    SET_DIALOG(state, payload) {
      const { component, params, config, listeners } = payload;
      state.dialogComponent = component;
      state.dialogParams = params;
      state.dialogConfig = config;
      state.dialogListeners = listeners;
      state.dialogVisible = true;
    },
  },
});


export default store;
