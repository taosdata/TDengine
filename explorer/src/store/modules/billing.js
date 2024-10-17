import { getBillingOverview, getPaymentMethod, updatePaymentMethod, addPaymentMethod } from "api/billing";
const state = {
  creditDialog: false,
  activeTab: "payment",
  overview: {},
  creditCardInfo: { cardName: "", cardNumber: "", cardMonth: "", cardYear: "", cardCvv: "" },
};
const mutations = {
  CHANGE_CREDIT_DIALOG(state, status) {
    state.creditDialog = status;
  },
  SET_OVERIVEW(state, data) {
    state.overview = data;
  },
};

const actions = {
  getBillingOverview({ commit }) {
    return getBillingOverview()
      .then(res => {
        commit("SET_OVERIVEW", res);
      })
      .catch(() => {
        commit("SET_OVERIVEW", {});
      });
  },
  getPaymentMethod() {
    return getPaymentMethod()
      .then(res => {
        res = typeof res !== "object" ? null : res;
        state.creditCardInfo = {
          ...res,
          cardMonth: res.cardExpMonth,
          cardYear: res.cardExpYear,
          cardCvv: res.cvcCode,
          cardNumber: "1234 1234 1234 " + res.cardNumber,
          cardName: res.cardOwnerName,
        };
      })
      .catch(() => {
        state.creditCardInfo = { cardName: "", cardNumber: "", cardMonth: "", cardYear: "", cardCvv: "" };
      });
  },
  updatePaymentMethod({ state }, data) {
    return state.creditCardInfo.cardNumber ? updatePaymentMethod(data) : addPaymentMethod(data);
  },
};

export default {
  namespaced: true,
  state,
  mutations,
  actions,
};
