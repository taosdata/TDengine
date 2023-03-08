//no-unused-vars
import router, { addRoutes } from "@/router/index.js";
import { getToken } from "@/utils/token.js";
import store from "./store";
import { NoInstanceAccessRoute, InitClusterStatus, BaseRoute, InactiveStatus } from "@/const";
const whiteList=['Login']

router.beforeEach(async (to, from, next) => {
  const hasToken = getToken();
  if(!hasToken){
    if(whiteList.includes(to.name)){
      next()
    }else{
      next(`/login`)
    }
  }
  // if (hasToken) {
  //   if (!store.state.app.userInfo) {
  //     // 获取用户信息
  //     await store.dispatch("app/getUserInfo");
  //     // 添加路由
  //     addRoutes(store.getters.role);
  //     // 获取region信息
  //     await store.dispatch("app/getCloud");
  //     await store.dispatch("app/getCurrentPricePlanList");
  //     // 获取集群列表，根据集群状态进行相应的跳转
  //     await store.dispatch("app/getClusterList");
  //     store.dispatch("app/getGlobalData");
  //     // 添加路由后必须步骤（除clusterStatus外）
  //     return next(handleReplacePath(to));
  //   }
  //   if (!store.getters.hasCluster && !NoInstanceAccessRoute.includes(to.path) && store.getters.role == "1") return next(NoInstanceAccessRoute[0]);
  //   if (InitClusterStatus.includes(store.getters.clusterStatus) && to.path != "/intanceStatus") return next("/intanceStatus");
  //   if (InactiveStatus.includes(store.getters.clusterStatus) && !BaseRoute.some(item => to.path.startsWith(item))) return next("/instances");
  //   if (NoInstanceAccessRoute.includes(to.path) && (store.getters.hasCluster || store.getters.role != "1")) return next("/instances");
  //   next();
  // } else {
  //   store.commit("app/LOGIN");
  // }
  next();
});
// 切换标签页之后返回页面，查询token
document.addEventListener("visibilitychange", () => {
  if (!document.hidden && !getToken()) {
    store.dispatch("app/logout", false);
  }
});
function handleReplacePath(to) {
  if (to.path == "/dashboard" && store.getters.role == "2") {
    return {
      path: "/explorer",
      replace: false,
    };
  }
  return {
    path: to.path,
    query: to.query,
    params: to.params,
    replace: true,
  };
}
// let timer = setInterval(()=>{
//   console.log(getToken(),'获取cookie');
// },1000)