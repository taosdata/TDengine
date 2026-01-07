//no-unused-vars
import router from '@/router/index';
import { getToken } from '@/utils/token';
import { getUrls } from '@/api/login';
import store from './store';
import { RouteLocationNormalized, RouteLocationNormalizedLoaded } from 'vue-router';
import { BaseUrlKey, OAuthTokenKey, SessionIdKey } from './const';
import Cookies from 'js-cookie';
import { getOAuthStatus, oauthMe } from './api/oauth';
import { encrypt } from '@/utils/index';
import aesCbcMac from './utils/aesCbcMac';
import pathDetector from './utils/pathDetector';

const apiPath = pathDetector.getApiBasePath();

// import pathDetector from './utils/pathDetector';
const whiteList = ['Login', 'OAuthCallback'];

router.beforeEach(async (to: RouteLocationNormalized, from: RouteLocationNormalizedLoaded, next) => {
  try {
    if (process.env.NODE_ENV === 'development') {
      console.log('router permission check:', to, from);
    }
    if (to.name != 'Login' && to.name != 'OAuthCallback') {
      if (store.state.app.isOAuthBinded) {
        return next();
      }
      if (to.query?.token) {
        console.log('Login with token', to.query.token);
        Cookies.set(SessionIdKey, to.query.token as string, { sameSite: 'lax', expires: 1 });
        await store.dispatch('app/setOAuthLogin', true);
      }
      try {
        console.log('try', store.state.app.isOAuthLogin);
        const user = await oauthMe(false);
        console.log('user', user);
        if (user.support_sync_users) {
          store.dispatch('app/setOAuthSyncUsersSupported', true);
        }
        if (user.tsdb_username) {
          // store.dispatch('app/setUsername', user.tsdb_username);
          localStorage.setItem('username', user.tsdb_username);
          const key = Cookies.get('encrypt_key') || '';
          if (key) {
            const encryptedPassword = aesCbcMac.decryptCbcMac(user.tsdb_password, key);
            const encryptedPwd = encrypt(encryptedPassword);
            localStorage.setItem('pwd', encryptedPwd);

            const SELF_PROVIDED = '__self__';
            if (user.provider !== SELF_PROVIDED) {
              await store.dispatch('app/setOAuthBinded', true);
            }
          }
        }
        if (user.user_id) {
          await store.dispatch('app/setOAuthLogin', true);
          return next();
        }
      } catch (error) {
        console.log('Login with oauth session error', error);
      }
      try {
        if (to.path.endsWith('/oauthLogin')) {
          const status = await getOAuthStatus();
          if (status?.enabled) {
            return (window.location.href = `${apiPath}/oauth/authorize`);
          }
        }
      } catch (error) {
        console.log('Login with oauth status error', error);
      }
      try {
        const result: ProfileResult = await getUrls();
        console.log(result);

        if (
          result.cluster != localStorage.getItem(BaseUrlKey) &&
          !localStorage.getItem(OAuthTokenKey) &&
          !store.state.app.isOAuthLogin
        ) {
          console.log('跳转登录');
          next(`/login`);
        }
      } catch (error) {
        console.log('获取url失败', error);
      }
      // Check for session-based authentication or OAuth login
      // Session validation happens on the backend via httpOnly cookies
      if (!store.state.app.loginWithSession) {
        if (whiteList.includes(to.name ? to.name.toString() : '')) {
          console.log('登录页面');
          next();
        } else {
          const user = await oauthMe(false);
          if (user.tsdb_username) {
            // store.dispatch('app/setUsername', user.tsdb_username);
            localStorage.setItem('username', user.tsdb_username);
            const key = Cookies.get('encrypt_key') || '';
            if (key) {
              const encryptedPassword = aesCbcMac.decryptCbcMac(user.tsdb_password, key);
              const encryptedPwd = encrypt(encryptedPassword);
              localStorage.setItem('pwd', encryptedPwd);

              await store.dispatch('app/setLoginWithSession', true);
              return next();
            }
          }
          console.log('无session跳转登录');
          next(`/login`);
        }
      }
    }
    next();
  } catch (error) {
    console.log('eeee', error);
  }
});
// 切换标签页之后返回页面，查询session
document.addEventListener('visibilitychange', () => {
  const hasSession = Cookies.get(SessionIdKey) || store.state.app.isOAuthLogin;
  if (!document.hidden && !getToken() && !hasSession) {
    store.dispatch('app/logout', false);
  }
});
