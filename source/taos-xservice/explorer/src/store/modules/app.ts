import { setToken, removeToken, removeAppID } from '@/utils/token.ts';
import { getAgentsData } from '@/api/agent';
import { objToLine } from '@/utils';
import { sendSQLReq } from '@/api/explorer';
import { getLocalTimezone } from '@/utils';
import { oauthLogout } from '@/api/oauth';
import Cookies from 'js-cookie';
import { OAuthTokenKey, SessionIdKey } from '@/const';
import router from '@/router';

const state = {
  // token: getToken(),
  loginInfo: {},
  language: import.meta.env.VUE_APP_LANGUAGE || 'en',
  userInfo: null,
  current_cluster: {},
  timeZone: getLocalTimezone(),
  appShow: true,
  mqttParser: null, //专供mqtt parser使用
  opcConfig: null, //opc的单例配置
  csvParser: null,
  csvtags: [], //用来保存csv的tag,有的时候是超级表，否则位普通表
  csvfiles: [],
  opcnodesfiles: [],
  opccertfiles: [],
  opcprivatefiles: [],
  hasheader: false,
  mqttcafile: [],
  mqttcertfile: [],
  mqttcertkeyfile: [],
  showcsvStable: false,
  agentLists: [],
  //以下四个是保存置顶的数据源的四个值(有两个ui界面导致的)
  currentDBName: '',
  currentAgentID: '',
  currentDBType: '',
  currentDSName: '',
  currentResume: 'always',
  currentEditID: '',
  agentDialog: false,
  transformExtractParseData: null,
  transformerFilterParseData: null,
  transformerMapColumns: null,
  transformerParserData: null,
  transformColumnIdentify: [],
  transformEchoMapData: null,
  csvTransformerParser: null,
  csvTransformerlocalCols: [], //csv无头部时候的自定义列
  splitExpressionList: null, //transformer的split
  mappingjoin: '', //mapping时候映射值是join时候的
  definitions: [],
  topParse: null,
  transformresulttable: [],
  createStWithoutDB: 0,
  transformTableHeight: 0,
  transformerfullparams: null,
  transresultname: '',
  activeColumns: [],
  resultCurrentPage: 1,
  showresulttb: false,
  resultTbTitle: '',
  historiandsn: '',
  historianechodata: null,
  connectivityCheckResult: {}, //连通性检查的结果
  complete: false, // 判断数据点位数据是否准备完成
  ticket: '',
  limitOffset: 5,
  showSystemMes: false, // 辅助判断是否展示联系团队的弹框
  validOpcFileRes: { valid: true },
  stbDefaultColumns: [], // transform 创建超级表时默认的列
  configData: [],
  activeName: 'datasource',
  viaId: null, // 点击数据源列表中的agent
  loginWithSession: false, // Login with session id cookie
  oauthEnabled: false, // OAuth is enabled on the backend
  isOAuthLogin: false, // User logged in via OAuth
  isOAuthBinded: false, // User logged in with TSDB credentials via OAuth
  isOAuthSyncUsersSupported: false, // Current OAuth config support sync-users API
  sysinfo: true // User has sysinfo permission
};

// 默认用户信息需要补充的字段
const defaultUserInfo = {
  phone: '',
  country_code: '',
  company_name: '',
  industry_type: '',
  position: '',
  cluster_url: ''
};

let timer = null;
const mutations = {
  SET_STB_DEFAULT_COLUMNS: (state, data) => {
    state.stbDefaultColumns = data;
  },
  SET_SHOW_SYSTEM_MES: (state, data) => {
    state.showSystemMes = data;
  },
  SET_VALDIT_OPC_FILE_RES: (state, data) => {
    state.validOpcFileRes = data;
  },
  SET_TICKET: (state, data) => {
    state.ticket = data;
  },
  SET_COMPLETE: (state, data) => {
    state.complete = data;
  },
  SET_CONNECTIVITY_CHECKRESULT: (state, data) => {
    state.connectivityCheckResult = data;
  },
  SET_HISTORIAN_ECHODATA: (state, data) => {
    state.historianechodata = data;
  },
  SET_HISTORIAN_DSN: (state, data) => {
    state.historiandsn = data;
  },
  SET_RESULTTB_SHOW: (state, data) => {
    state.showresulttb = data;
  },
  SET_RESULTTB_TITLE_SHOW: (state, data) => {
    state.resultTbTitle = data;
  },
  SET_LIMIT_OFFSET: (state, data) => {
    state.limitOffset = data;
  },
  SET_RESULT_PAGE: (state, data) => {
    state.resultCurrentPage = data;
  },
  SET_ACTIVE_COLS: (state, data) => {
    state.activeColumns = data;
  },
  SET_TRANS_RESULT_NAME: (state, data) => {
    state.transresultname = data;
  },
  SET_TRANS_FULL_PARAMS: (state, data) => {
    state.transformerfullparams = data;
  },
  SET_TRANS_TABLE_HEIGHT: (state, data) => {
    state.transformTableHeight = data;
  },
  SET_CREATESTWITHOUT_DB: (state, data) => {
    state.createStWithoutDB = data;
  },
  SET_TRANS_RESULT_TABLE: (state, data) => {
    state.transformresulttable = data;
  },
  SET_TOP_PARSE: (state, data) => {
    state.topParse = data;
  },
  SET_MAPPING_JOIN: (state, data) => {
    state.mappingjoin = data;
  },
  SET_SPLIT_EXPRESS: (state, data) => {
    state.splitExpressionList = data;
  },
  SET_CSV_LOCAL_COLS: (state, data) => {
    state.csvTransformerlocalCols = data;
  },
  SET_CSV_TRANSFORMER_PARSER: (state, data) => {
    state.csvTransformerParser = data;
  },
  SET_FILTER_PARSE_DATA: (state, data) => {
    state.transformerFilterParseData = data;
  },
  SET_EXTRACT_PARSE_DATA: (state, data) => {
    state.transformExtractParseData = data;
  },
  SET_ECHO_MAP_DATA: (state, data) => {
    state.transformEchoMapData = data;
  },
  SET_TRANSFORM_COL_IDENTIFIED: (state, data) => {
    state.transformColumnIdentify = data;
  },
  SET_TRANSFORM_PARSERDATA: (state, data) => {
    state.transformerParserData = data;
  },
  SET_TRANSFORMER_MAPCOLUMNS: (state, data) => {
    state.transformerMapColumns = data;
  },
  SET_AGENT_DIALOG: (state, data) => {
    state.agentDialog = data;
  },
  SET_CURRENT_EDITID: (state, data) => {
    state.currentEditID = data;
  },
  SET_CURRENT_DBNAME: (state, data) => {
    state.currentDBName = data;
  },
  SET_CURRENT_AGENT: (state, data) => {
    state.currentAgentID = data;
  },
  SET_CURRENT_DSNAME: (state, data) => {
    state.currentDSName = data;
  },
  SET_CURRENT_DBTYPE: (state, data) => {
    state.currentDBType = data;
    state.supportSQL =
      data == 'avevaHistorian' || data == 'mysql' || data == 'postgres' || data == 'oracle' || data == 'mssql';
    state.supportTransform =
      data == 'avevaHistorian' ||
      data == 'mysql' ||
      data == 'postgres' ||
      data == 'oracle' ||
      data == 'mssql' ||
      data == 'kafka' ||
      data == 'mqtt' ||
      data == 'mongodb';
  },
  SET_CURRENT_RESUME: (state, data) => {
    state.currentResume = data;
  },
  SET_AGENT_LISTS: (state, data) => {
    state.agentLists = data;
  },
  SET_DEFINITIONS(state, definitions) {
    state.definitions = definitions;
  },
  //所有数据源上传的文件类型置空
  SET_FILE_EMPTY: (state, data) => {
    state.csvfiles = data;
    state.opcnodesfiles = data;
    state.opccertfiles = data;
    state.mqttcafile = data;
    state.mqttcertfile = data;
    state.mqttcertkeyfile = data;
  },
  SET_SHOW_CSV_STABLE: (state, data) => {
    state.showcsvStable = data;
  },
  SET_CSV_TAGS: (state, data) => {
    state.csvtags = data;
  },
  SET_MQTT_CAFILE: (state, data) => {
    state.mqttcafile = data;
  },
  SET_MQTT_CERTFILE: (state, data) => {
    state.mqttcertfile = data;
  },
  SET_MQTT_CERTKEYFILE: (state, data) => {
    state.mqttcertkeyfile = data;
  },
  SET_OPC_PRIVATEFILES: (state, data) => {
    state.opcprivatefiles = data;
  },
  SET_OPC_CERTFILES: (state, data) => {
    state.opccertfiles = data;
  },
  SET_OPC_UANODES: (state, data) => {
    state.opcnodesfiles = data;
  },
  SET_CSV_HASHEADER: (state, data) => {
    state.hasheader = data;
  },
  SET_CSV_FILES: (state, data) => {
    state.csvfiles = data;
  },
  SET_CSV_PARSER: (state, data) => {
    state.csvParser = data;
  },
  SET_OPC_CONFIG: (state, data) => {
    state.opcConfig = data;
  },
  SET_MQTT_PARSER: (state, data) => {
    state.mqttParser = data;
  },
  SET_CLUSTER_URL(state, url) {
    state.cluster_url = url;
  },
  SAVE_LOGIN_INFO(state, info) {
    state.loginInfo = info;
  },
  SET_TIME_ZONE(state, timeZone) {
    state.timeZone = timeZone;
  },
  SET_TOKEN: (state, token) => {
    state.token = token;
    setToken(token);
  },
  SET_LOGIN_WITH_SESSION: (state: { loginWithSession: boolean }, loginWithSession: boolean) => {
    state.loginWithSession = loginWithSession;
  },

  SET_USERINFO: (state, userInfo) => {
    if (!userInfo) return (state.userInfo = userInfo);
    state.userInfo = { ...defaultUserInfo, ...objToLine(userInfo) };
  },
  CLEAR_TIMEOUT() {
    timer && clearTimeout(timer);
    timer = null;
  },
  LOGIN() {
    // window.location.href = import.meta.env.VUE_APP_LOGIN_URL;
  },
  SET_OAUTH_ENABLED: (state, enabled: boolean) => {
    state.oauthEnabled = enabled;
  },
  SET_OAUTH_LOGIN: (state, isOAuth: boolean) => {
    state.isOAuthLogin = isOAuth;
  },
  SET_OAUTH_BINDED: (state, isOAuthBinded: boolean) => {
    state.isOAuthBinded = isOAuthBinded;
  },
  SET_OAUTH_SYNC_USERS_SUPPORTED: (state, isOAuthSyncUsersSupported: boolean) => {
    state.isOAuthSyncUsersSupported = isOAuthSyncUsersSupported;
  },
  SET_SYSINFO: (state, sysinfo) => {
    state.sysinfo = sysinfo;
  }
};

const actions = {
  getAgentList({ commit }) {
    return getAgentsData().then(res => {
      commit('SET_AGENT_LISTS', res);
    });
  },
  async getUserInfo({ commit }) {
    if (!state.userInfo) {
      const userName = localStorage.getItem('username');
      await sendSQLReq(`select * from information_schema.ins_users where name='${userName}'`)
        .then(res => {
          const user = res.data.map(data => {
            return Object.fromEntries(
              res.column_meta.map((item, index) => {
                return [item[0], data[index]];
              })
            );
          });
          commit('SET_USERINFO', user[0]);
        })
        .catch(err => {
          return Promise.reject(err);
        });
    }
    return state.userInfo;
  },
  async logout({ commit, state }, request = true, ssoLogout = false) {
    console.log('logout', state);
    commit('SET_TOKEN', '');
    commit('CLEAR_TIMEOUT');
    removeAppID();
    commit('LOGIN', request);
    commit('SET_USERINFO');
    // Clean up OAuth-related cookies
    Cookies.remove(OAuthTokenKey);
    Cookies.remove(SessionIdKey);

    // If the user logged in via OAuth (server-side session), call backend logout
    // to invalidate the httpOnly session cookie. Do not rely on localStorage-stored tokens.
    if (state) {
      try {
        // If the backend endpoint exists, this will clear the session cookie.
        // The request util will include credentials for OAuth mode.
        await oauthLogout();
      } catch (e) {
        // Ignore network errors during logout; proceed with client-side cleanup.
        // eslint-disable-next-line no-console
        console.warn('oauthLogout failed', e);
      }
      commit('SET_OAUTH_LOGIN', false);
      commit('SET_OAUTH_BINDED', false);
      commit('SET_LOGIN_WITH_SESSION', false);
    }

    // Clear client-side OAuth flag (do not remove oauth token from localStorage here;
    // we no longer rely on localStorage for OAuth authentication).
    commit('SET_OAUTH_LOGIN', false);
    router.push('/login');
  },
  setOAuthEnabled({ commit }, enabled = true) {
    commit('SET_OAUTH_ENABLED', enabled);
  },
  setOAuthLogin({ commit }, isOAuth = true) {
    commit('SET_OAUTH_LOGIN', isOAuth);
  },
  setOAuthBinded({ commit }, isOAuthBinded = true) {
    commit('SET_OAUTH_BINDED', isOAuthBinded);
  },
  setOAuthSyncUsersSupported({ commit }, isOAuthSyncUsersSupported = true) {
    commit('SET_OAUTH_SYNC_USERS_SUPPORTED', isOAuthSyncUsersSupported);
  }
};

export default {
  namespaced: true,
  state,
  mutations,
  actions
};
