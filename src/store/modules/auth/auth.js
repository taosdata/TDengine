// import { setToken } from '@/utils/token'
import { loginReq, register, change } from "@/api/auth";
import router from "@/router";
const state = {};

const mutations = {};
const actions = {
  login({ commit }, payload) {
    return new Promise((_, reject) => {
      loginReq({ email: payload.email, password: payload.password })
        .then(data => {
          payload.callback && payload.callback(data);
          let { state, token, tokenType } = data;
          token = tokenType + " " + token;
          // TODO
          commit("app/SET_TOKEN", token, { root: true });
          // setToken(token)
          if (state == 2) {
            // 判断用户状态，如果是2， 跳转到引导页，完善信息
            router.push("/register");
          } else if (state == 1) {
            // 用户状态，如果是1，信息已经完善，跳转到首页
            router.push("/");
          }
        })
        .catch(async res => {
          // 未激活，跳转到引导页
          if (res.code === 11436) {
            router.push("/auth/guide/" + payload.email);
          } else if (res.code === 11410) {
            // 密码强度不合法
            reject("Password does not meet requirements");
          } else if (res.code === 11501) {
            // 邮箱已激活，密码错误
            reject("wrong user name or password");
          } else {
            // Message({
            //   message: "login fail",
            //   type: "error",
            //   duration: 3000,
            // });
            reject(res?.msg || res.message);
          }
        });
    });
  },
  register(_, payload) {
    return new Promise((_, rej) => {
      register(payload)
        .then(() => {
          router.push("/auth/guide/" + payload.email);
        })
        .catch(err => {
          rej(err?.message || err.msg);
        });
    });
  },
  change(_, payload) {
    return new Promise((res, rej) => {
      change(payload)
        .then(() => {
          // router.push("/auth/login");
          res();
        })
        .catch(err => {
          console.log(err.msg);
          rej(err?.message || err.msg);
        });
    });
  },
};

export default {
  namespaced: true,
  state,
  mutations,
  actions,
};
