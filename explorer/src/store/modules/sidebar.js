import Cookies from "js-cookie"

const state = {
  opened: Cookies.get('sidebarStatus') ? !!+Cookies.get('sidebarStatus') : true,
}

const mutations = {
  TOGGLE_SIDEBAR: state => {
    state.opened = !state.opened
    if(state.opened){
      Cookies.set('sidebarStatus', 1)
    } else {
      Cookies.set('sidebarStatus', 0)
    } 
  },
}

const actions = {

}

export default {
  namespaced: true,
  state, 
  mutations,
  actions
}