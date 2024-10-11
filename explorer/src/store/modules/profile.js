import { Message } from "element-ui";
import { getCountryList, getProfessionList, getPositionList } from "@/api/register";
import { putUserInfo } from "@/api/auth";
const state = {
  positions: [],
  industrys: [],
  countrys: [],
};

const mutations = {
  SET_COUNTRY_LIST: (state, countrys) => {
    state.countrys = countrys;
  },
  SET_INDUSTRY_LIST: (state, industrys) => {
    state.industrys = industrys;
  },
  SET_POSITION_LIST: (state, positions) => {
    state.positions = positions;
  },
};

const actions = {
  // TODO: 三合一，防止多个报错
  getCountryList({ commit, rootState }) {
    getCountryList()
      .then(country_list => {
        let country = rootState.language == "en" ? "US" : "CN";
        let countryIndex = country_list.findIndex(item => item.value == country);
        country = country_list[countryIndex];
        country_list.splice(countryIndex, 1);
        country_list.unshift(country);
        commit("SET_COUNTRY_LIST", country_list);
      })
      .catch(err => {
        Message({
          message: err.message,
          type: "error",
          duration: 3000,
        });
      });
  },
  getIndustryList({ commit }) {
    getProfessionList()
      .then(industry_list => {
        commit("SET_INDUSTRY_LIST", industry_list);
      })
      .catch(err => {
        Message({
          message: err.message,
          type: "error",
          duration: 3000,
        });
      });
  },
  getPositionList({ commit }) {
    getPositionList()
      .then(position_list => {
        commit("SET_POSITION_LIST", position_list);
      })
      .catch(err => {
        Message({
          message: err.message,
          type: "error",
          duration: 3000,
        });
      });
  },
  putUserInfo({ dispatch, rootState }, payload) {
    return putUserInfo(payload, rootState.app.userInfo.email).then(() => {
      dispatch("app/getUserInfo", null, { root: true });
    });
  },
};

export default {
  namespaced: true,
  state,
  mutations,
  actions,
};
