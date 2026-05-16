const getters = {
  token: state => state.app.token,
  appId: state => state.app.current_cluster.id || "1597864550720372736",
  role: state => state.app.userInfo?.role_id || "1",
  userInfo: state => state.app.userInfo,
};

export default getters;
