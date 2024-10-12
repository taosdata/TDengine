const getters = {
  token: state => state.app.token,
  phonePre: state => {
    return state.profile.countrys.find(item => item.value == state.app.userInfo.country_code)?.dialing || "+86";
  },
  operate(state) {
    // 如果集群状态只有为running才可以进行操作
    return state.app.clusterStatus == "Running";
  },
  appId: state => state.app.current_cluster.id || "1597864550720372736",
  role: state => state.app.userInfo?.role_id || "1",
  currentCloudAndRegion: state => state.app.currentCloudAndRegion,
  userInfo: state => state.app.userInfo,
  currentPricePlan: state => state.app.currentPricePlan,
  currentServerLevel: state => state.app.currentServerLevel,
  hasCluster: state => !!state.app.clusters.length,
};

export default getters;
