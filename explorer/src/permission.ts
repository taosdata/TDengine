//no-unused-vars
import router from "@/router/index";
import { getToken } from "@/utils/token";
import { getUrls } from "@/api/login";
import store from "./store";

const whiteList = ["Login"];

router.beforeEach(async (to, from: any, next) => {
  try {
    if (to.name != "Login") {
      const result: Recordable<ProfileResult> = await getUrls();

      if (
        result?.cluster != localStorage.getItem("base_url") &&
        to.name != "Login"
      ) {
        next(`/login`);
      }
      const hasToken = getToken();
      if (!hasToken) {
        if (whiteList.includes(to.name)) {
          console.log('登录页面');
          next();
        } else {
          next(`/login`);
        }
      }
    }
    next();
  } catch (error) {
    console.log('eeee', error)
  }
});
// 切换标签页之后返回页面，查询token
document.addEventListener("visibilitychange", () => {
  if (!document.hidden && !getToken()) {
    store.dispatch("app/logout", false);
  }
});

