//no-unused-vars
import router, { addRoutes } from "@/router/index.js";
import { getToken } from "@/utils/token.js";
import { getUrls } from "@/api/explorer/login";
import store from "./store";

const whiteList = ["Login"];

router.beforeEach(async (to, from, next) => {
  let result = await getUrls();

  if (
    result.cluster != localStorage.getItem("base_url") &&
    to.name != "Login"
  ) {
    next(`/login`);
    next();
  }
  const hasToken = getToken();
  if (!hasToken) {
    if (whiteList.includes(to.name)) {
      next();
    } else {
      next(`/login`);
    }
  }
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

