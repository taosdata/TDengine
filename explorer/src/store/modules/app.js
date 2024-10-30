import i18n from "@/lang";
import {
  getToken,
  setToken,
  removeToken,
  getAppID,
  setAppId,
  removeAppID,
  setRedirect,
} from "@/utils/token";
import {
  getAgentsData
} from "@/api/explorer/agent";
import { objToLine } from "@/utils";
import {
  getClusterListReq,
  getClusterStatus,
  getPlan,
} from "@/api/gateway/app";
import { getUserInfoReq, logout } from "@/api/auth";
import { getClusterInfo } from "@/api/dashboard";
import { getAlertList } from "@/api/gateway/alert";
import { setLang } from "@/lang";
import { getCloudRegion } from "@/api/register";
import {
  BaseRoute,
  ServerLevel,
  NeedRefreshStatus,
  InitClusterStatus,
} from "@/const";
import router from "@/router";
import { Message } from "element-ui";
import moment from "moment";
import { sendSQLReq } from "@/api/gateway/console";
import { getLocalTimezone } from "@/utils";

const state = {
  token: getToken(),
  loginInfo: {},
  language: process.env.VUE_APP_LANGUAGE || "en",
  userInfo: null,
  clusters: [],
  currentRegionClusterList: [],
  currentCloudAndRegion: {},
  current_cluster: {},
  regionUrlMap: {},
  cluster_info: {},
  timeZone: getLocalTimezone(),
  clusterStatus: "Suspended",
  appShow: true,
  isGuide: false,
  newAlert: [],
  cloudList: [],
  cloud: "", //当前服务的云区域
  currentServerLevel: 0, //当前服务的服务等级
  currentPricePlan: null, //当前账号的计费方案
  currentPricePlanList: [], //当前region的计费方案列表
  mqttParser: null, //专供mqtt parser使用
  opcConfig: null, //opc的单例配置
  csvParser: null,
  csvtags:[],//用来保存csv的tag,有的时候是超级表，否则位普通表
  csvfiles: [],
  csvFileListener: {},
  opcnodesfiles: [],
  opccertfiles: [],
  opcprivatefiles: [],
  hasheader: false,
  mqttcafile: [],
  mqttcertfile: [],
  mqttcertkeyfile: [],
  showcsvStable:false,
  agentLists:[],
  //以下四个是保存置顶的数据源的四个值(有两个ui界面导致的)
  currentDBName:'',
  currentAgentID:'',
  currentDBType:'',
  currentDSName:'',
  currentResume: 'always',
  currentEditID:'',
  agentDialog:false,
  transformExtractParseData:null,
  transformerFilterParseData:null,
  transformerMapCloumns:null,
  transformerParserData:null,
  transformColumnIdentify:[],
  transformEchoMapData:null,
  csvTransformerParser:null,
  csvTransformerlocalCols:[],//csv无头部时候的自定义列
  splitExpresList:null,//transformer的split
  mappingjoin:'',//mapping时候映射值是join时候的
  definitions: [],
  topParse:null,
  transformresulttable:[],
  createStWithoutDB:0,
  transformTableHeight:0,
  transformerfullparams:null,
  transresultname:'',
  activeColumns:[],
  resultCurrentPage:1,
  showresulttb:false,
  resultTbTitle: '',
  historiandsn:'',
  historianechodata:null,
  connectivityCheckResult: {}, //连通性检查的结果
  complete: false,// 判断数据点位数据是否准备完成 
  ticket: '',
  limitOffset: 5,
  showSystemMes: false, // 辅助判断是否展示联系团队的弹框
  validOpcFileRes: { valid: true },
  stbDefaultColumns: [],// transfrom 创建超级表时默认的列
  configData: [],
  activeName: 'datasource',
  viaId: null, // 点击数据源列表中的agent
};
const saveKey = encodeURIComponent("appId");
const waitTime = 15 * 60 * 1000;

// 默认用户信息需要补充的字段
const defaultUserInfo = {
  phone: "",
  country_code: "",
  company_name: "",
  industry_type: "",
  position: "",
  cluster_url: "",
};
// const currentHost = new RegExp("^(https?://)?" + window.location.host + "$");
const currentHost = {
  host: window.location.host,
  test(host) {
    return this.host == host;
  },
};
// 记录集群列表刷新次数
let refreshCount = 0;
// 集群列表刷新时间
const refresTime = 15000;
let timer = null;
const mutations = {
  SET_STB_DEFAULT_COLUMNS:(state,data) => {
    state.stbDefaultColumns=data
  },
  SET_SHOW_SYSTEM_MES:(state,data) => {
    state.showSystemMes=data
  },
  SET_VALDIT_OPC_FILE_RES:(state,data) => {
    state.validOpcFileRes=data
  },
  SET_TICKET:(state,data) => {
    state.ticket=data
  },
  SET_COMPLETE:(state,data) => {
    state.complete=data
  },
  SET_CONNECTIVITY_CHECKRESULT:(state,data) => {
    state.connectivityCheckResult=data
  },
  SET_HISTORIAN_ECHODATA:(state,data)=>{
    state.historianechodata=data
  },
  SET_HISTORIAN_DSN:(state,data)=>{
    state.historiandsn=data
  },
  SET_RESULTTB_SHOW:(state,data)=>{
    state.showresulttb=data
  },
  SET_RESULTTB_TITLE_SHOW:(state,data)=>{
    state.resultTbTitle=data
  },
  SET_LIMIT_OFFSET:(state,data)=>{
    state.limitOffset=data
  },
  SET_RESULT_PAGE:(state,data)=>{
    state.resultCurrentPage=data
  },
  SET_ACTIVE_COLS:(state,data)=>{
    state.activeColumns=data
  },
  SET_TRANS_RESULT_NAME:(state,data)=>{
    state.transresultname=data
  },
  SET_TRANS_FULL_PARAMS:(state,data)=>{
    state.transformerfullparams=data
  },
  SET_TRANS_TABLE_HEIGHT:(state,data)=>{
    state.transformTableHeight=data
  },
  SET_CREATESTWITHOUT_DB:(state,data)=>{
    state.createStWithoutDB=data
  },
  SET_TRANS_RESULT_TABLE:(state,data)=>{
    state.transformresulttable=data
  },
  SET_TOP_PARSE:(state,data)=>{
    state.topParse=data
  },
  SET_MAPPING_JOIN:(state,data)=>{
    state.mappingjoin=data
  },
  SET_SPLIT_EXPRESS:(state,data)=>{
    state.splitExpresList=data
  },
  SET_CSV_LOCAL_COLS:(state,data)=>{
    state.csvTransformerlocalCols=data
  },
  SET_CSV_TRANSFORMER_PARSER:(state,data)=>{
    state.csvTransformerParser=data
  },
  SET_FILTER_PARSE_DATA:(state,data)=>{
    state.transformerFilterParseData=data
  },
  SET_EXTRACT_PARSE_DATA:(state,data)=>{
    state.transformExtractParseData=data
  },
  SET_ECHO_MAP_DATA:(state,data)=>{
    state.transformEchoMapData=data
  },
  SET_TRANSFORM_COL_IDENTIFIED:(state,data)=>{
    state.transformColumnIdentify=data
  },
  SET_TRANSFORM_PARSERDATA:(state,data)=>{
    state.transformerParserData=data
  },
  SET_TRANSFORMER_MAPCOLUMNS:(state,data)=>{
    state.transformerMapCloumns=data
  },
  SET_AGENT_DIALOG:(state,data)=>{
    state.agentDialog=data
  },
  SET_CURRENT_EDITID:(state,data)=>{
    state.currentEditID=data
  },
  SET_CURRENT_DBNAME:(state,data)=>{
    state.currentDBName=data
  },
  SET_CURRENT_AGENT:(state,data)=>{
    state.currentAgentID=data
  },
  SET_CURRENT_DSNAME:(state,data)=>{
    state.currentDSName=data
  },
  SET_CURRENT_DBTYPE:(state,data)=>{
    state.currentDBType=data
    state.supportSQL = (data == 'avevaHistorian' || data == 'mysql' || data == 'postgres' || data == 'oracle' || data == "mssql")
    state.supportTransform = (data == 'avevaHistorian' || data == 'mysql' || data == 'postgres' || data == 'oracle' || data == "mssql" || data == "kafka" || data == "mqtt" || data == "mongodb")
  },
  SET_CURRENT_RESUME:(state,data)=>{
    state.currentResume=data
  },
  SET_AGENT_LISTS:(state,data)=>{
    state.agentLists=data
  },
  SET_DEFINITIONS(state, definitions) {
    state.definitions = definitions;
  },
  //所有数据源上传的文件类型置空
  SET_FILE_EMPTY:(state,data)=>{
    state.csvfiles=data
    state.opcnodesfiles=data
    state.opccertfiles=data
    state.mqttcafile=data
    state.mqttcertfile=data
    state.mqttcertkeyfile=data
  },
  SET_SHOW_CSV_STABLE:(state,data)=>{
    state.showcsvStable=data
  },
  SET_CSV_TAGS:(state,data)=>{
    state.csvtags=data
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
  SET_CSV_FILE_LISTENER: (state, data) => {
    state.csvFileListener = data;
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
    if (!token) removeToken();
    setToken(token);
  },

  SET_USERINFO: (state, userInfo) => {
    if (!userInfo) return (state.userInfo = userInfo);
    state.userInfo = { ...defaultUserInfo, ...objToLine(userInfo) };
  },
  SET_CLUSTER_INFO(state, cluster_info) {
    state.cluster_info = cluster_info;
  },
  SET_CLUSTER_LIST: (state, clusters) => {
    state.clusters = clusters;
  },
  SET_CURRENT_SERVER_LEVEL: (state, level) => {
    state.currentServerLevel = isNaN(level) ? 0 : level;
  },
  SET_CURRENT_PRICE_PLAN(state, currentPricePlan) {
    state.currentPricePlan = currentPricePlan;
  },
  SET_CURRENT_CLUSTER(state, current_cluster) {
    const isFirstCreate = window.location.pathname == "/createFirstInstance";
    if (current_cluster?.id) {
      /**
       * 获取当前集群的url进行比较和切换
       */
      const urlKey = current_cluster.cloud_id + "_" + current_cluster.region_id;
      const url = state.regionUrlMap[urlKey];
      const completeUrl = window.location.origin.replace(currentHost.host, url);
      completeUrl && setRedirect(completeUrl);
      setAppId(
        current_cluster.id,
        process.env.NODE_ENV != "development" ? url : undefined
      );

      if (
        process.env.NODE_ENV != "development" &&
        url &&
        !currentHost.test(url)
      ) {
        if (InitClusterStatus.includes(current_cluster.cluster_status)) {
          window.location.href =
            completeUrl +
            "/instanceStatus" +
            (isFirstCreate ? "?isFirstCreate=true" : "");
        } else {
          window.location.href = window.location.href.replace(
            currentHost.host,
            url
          );
        }
        return;
      }
      this.commit("app/CLEAR_TIMEOUT");
      saveUserAppToSessionStorage(current_cluster);
    } else {
      removeAppID();
    }
    state.current_cluster = current_cluster || {};
    state.clusterStatus = current_cluster?.cluster_status || "Suspended";
    handleClusterStatus(current_cluster, isFirstCreate, isFirstCreate);
  },
  CLEAR_TIMEOUT() {
    timer && clearTimeout(timer);
    timer = null;
    refreshCount = 0;
  },
  LOGIN() {
    // window.location.href = process.env.VUE_APP_LOGIN_URL;
  },
};

const actions = {
  getAgentList({ commit }) {
    return getAgentsData().then(res => {
      commit('SET_AGENT_LISTS', res);
    });
  },
  async getUserInfo({ commit, dispatch }) {
    if (!state.userInfo) {
      let userName = localStorage.getItem("username");
      await sendSQLReq(
        `select * from information_schema.ins_users where name='${userName}'`
      )
        .then((res) => {
          let user = res.data.map((data) => {
            return Object.fromEntries(
              res.column_meta.map((item, index) => {
                return [item[0], data[index]];
              })
            );
          });
          commit("SET_USERINFO", user[0]);
        })
        .catch((err) => {
          return Promise.reject(err);
        });
    }
    return state.userInfo;
  },
  getGlobalData({ dispatch }) {
    dispatch("profile/getCountryList", null, { root: true });
    dispatch("profile/getPositionList", null, { root: true });
    dispatch("profile/getIndustryList", null, { root: true });
    dispatch("billing/getPaymentMethod", null, { root: true });
    dispatch("getNewAlert", true);
  },
  getCloud({ state }) {
    return getCloudRegion()
      .then((res) => {
        state.cloudList = res;
        /**
         * 根据当前url的hostname获取对应的cloud和region
         * 如果没找到就跳转第一个region的url
         */
        // 开发环境不跳转
        if (process.env.NODE_ENV == "development") {
          let cloud = res[0];
          let region = cloud.regions[0];
          state.currentCloudAndRegion = {
            cloud: cloud.label,
            cloudId: cloud.value,
            region: region.label,
            regionId: region.value,
          };
        }
        res.forEach((item) => {
          item.regions.forEach((region) => {
            let url = region.url?.replace(/https?:\/\/([^/]+).*/, "$1");
            state.regionUrlMap[item.value + "_" + region.value] = url;
            if (currentHost.test(url)) {
              state.currentCloudAndRegion = {
                cloud: item.label,
                cloudId: item.value,
                region: region.label,
                regionId: region.value,
              };
            }
          });
        });
        if (!state.currentCloudAndRegion.cloud) {
          this.$error(i18n.t("cloudError"));
        }
      })
      .catch(() => {
        state.cloudList = [];
        state.currentCloudAndRegion = {};
      });
  },
  getCurrentPricePlanList({ state }) {
    return getPlan({
      cloudId: state.currentCloudAndRegion.cloudId,
      regionId: state.currentCloudAndRegion.regionId,
      planType: "BASE",
    })
      .then((data) => {
        state.currentPricePlanList = data;
      })
      .catch(() => {
        state.currentPricePlanList = [];
      });
  },
  setPricePlan({ state, commit }) {
    // 查找当前账户的计费方案
    let currentPriceLevel = getCurrentPricePlan();

    commit("SET_CURRENT_SERVER_LEVEL", Number(ServerLevel[currentPriceLevel]));
    commit(
      "SET_CURRENT_PRICE_PLAN",
      state.currentPricePlanList.find(
        (item) => item.priceLevel == currentPriceLevel
      )
    );
  },
  getClusterList({ state, commit, dispatch }, isRefresh = true) {
    commit("CLEAR_TIMEOUT");
    return new Promise((resolve) => {
      let fn = () => {
        getClusterListReq()
          .then((cluster_list) => {
            commit("SET_CLUSTER_LIST", cluster_list);
            // 无集群列表处理
            if (!cluster_list.length || !state.currentCloudAndRegion.cloud) {
              state.currentRegionClusterList = [];
              commit("SET_CURRENT_CLUSTER", {});
              resolve();
              return;
            }
            let cluster = handleCluster();
            commit("SET_CURRENT_CLUSTER", cluster);
            resolve();
            // 刷新时需要判断集群列表中是否有除了Running、Suspened的状态的集群
            let refresh = cluster_list.some((item) =>
              NeedRefreshStatus.includes(item.cluster_status)
            );
            if (refresh && isRefresh) {
              refreshCount++;
              // 超过20次刷新，则不再刷新
              if (refreshCount <= 20) {
                return (timer = setTimeout(fn, refresTime));
              }
            }
            commit("CLEAR_TIMEOUT");
          })
          .catch(() => {
            commit("SET_CLUSTER_LIST", []);
            commit("SET_CURRENT_CLUSTER", {});
            resolve();
          })
          .then(() => {
            refreshCount < 2 && dispatch("setPricePlan");
          });
      };
      fn();
    });
  },
  getClusterInfo({ commit }) {
    return getClusterInfo({
      from: moment.utc().format("YYYY-MM") + "-01",
    }).then((res) => {
      commit("SET_CLUSTER_INFO", res);
    });
  },
  logout({ commit }, request = true) {
    commit("SET_TOKEN", "");
    commit("CLEAR_TIMEOUT");
    removeAppID();
    commit("LOGIN", request);
    commit("SET_USERINFO");
  },
  // logout({ commit }, request = true) {
  //   const run = () => {
  //     // 退出就得置空
  //     commit("SET_TOKEN", "");
  //     commit("CLEAR_TIMEOUT");
  //     removeAppID();
  //     commit("LOGIN", request);
  //   };
  //   if (!request) return run();
  //   logout().finally(run).catch(err=>err);
  // },
  getNewAlert({ state }, autoRefresh = false) {
    let fn = () => {
      return getAlertList(
        {
          page_size: 10,
          status: 0,
        },
        true
      )
        .then((res) => {
          state.newAlert = res.content;
          autoRefresh && setTimeout(fn, refresTime);
        })
        .catch(() => (state.newAlert = []));
    };
    return fn();
  },
  getClusterStatus({ state, commit, dispatch }, app_id) {
    // 如果没有传入appid则为页面加载第一次请求状态
    app_id = app_id || state.current_cluster.id;
    commit("CLEAR_TIMEOUT");
    return new Promise((resolve, reject) => {
      let start = Date.now();
      let fn = () => {
        if (state.current_cluster.id != app_id) return;
        getClusterStatus(app_id)
          .then((res) => {
            state.clusterStatus = res;
            if (res == "Running") {
              setTimeout(async () => {
                await dispatch("getClusterList");
                resolve();
              }, 2000);
            } else if (res == "Suspended") {
              // 如果状态为停用直接允许跳转
              resolve(true);
            } else {
              if (Date.now() - start > waitTime) {
                commit("CLEAR_TIMEOUT");
                dispatch("getClusterList");
                router.push("/instances");
              } else {
                timer = setTimeout(() => {
                  fn();
                }, 5000);
              }
            }
          })
          .catch((err) => {
            reject(err);
          });
      };
      fn();
    });
  },
};
export function saveUserAppToSessionStorage(cluster) {
  let obj = JSON.parse(sessionStorage.getItem(saveKey)) || {};
  obj[state.userInfo.account_id] = cluster.id;
  sessionStorage.setItem(saveKey, JSON.stringify(obj));
}
export function delUserAppInSessionStorage() {
  let obj = JSON.parse(sessionStorage.getItem(saveKey)) || {};
  obj[state.userInfo.account_id] = "";
  sessionStorage.setItem(saveKey, JSON.stringify(obj));
}
export function getAppFromSessionStorage() {
  let obj = JSON.parse(sessionStorage.getItem(saveKey)) || {};
  return obj[state.userInfo.account_id] || "";
}
function handleClusterStatus(cluster, isFirstCreate = false) {
  if (!cluster) return;
  const path = window.location.pathname;
  let clusterStatus = cluster.cluster_status;
  if (clusterStatus == "Running") return;
  if (InitClusterStatus.includes(clusterStatus) && path != "/instanceStatus") {
    return routerReplace(
      "/instanceStatus" + (isFirstCreate ? "?isFirstCreate=true" : "")
    );
  }
  // 其他状态暂时统一处理
  if (!BaseRoute.some((item) => path.includes(item))) {
    routerReplace("/instances");
  }
}
function routerReplace(url) {
  router.replace(url).catch(() => {
    router.onReady(() => {
      let currentUrl = window.location.pathname + window.location.search;
      if (currentUrl != url) {
        router.replace(url);
      }
    });
  });
}
function handleCluster() {
  /**
   * 1.先从cookie中获取,如果没有则从sessionStorage中获取
   * 2.如果没有就获取当前url的region下的一个集群（先查找running的，没有就取第一个）
   * 3.如果当前url没有找到归属的region，就不设置集群
   */
  if (!refreshCount) {
    getCurentRegionCluster();
  }
  if (!state.currentRegionClusterList.length) {
    return {};
  }
  if (!state.current_cluster.id) {
    state.current_cluster.id = getAppID() || getAppFromSessionStorage();
  }
  return (
    state.currentRegionClusterList.find(
      (item) => item.id == state.current_cluster.id
    ) ||
    state.currentRegionClusterList.find(
      (item) => item.cluster_status == "Running"
    ) ||
    state.currentRegionClusterList[0]
  );
}
function getCurentRegionCluster() {
  let cloud = state.currentCloudAndRegion.cloudId;
  let region = state.currentCloudAndRegion.regionId;
  state.currentRegionClusterList = state.clusters.filter(
    (item) => item.cloud_id == cloud && item.region_id == region
  );
}

function getCurrentPricePlan() {
  let priceServel = 0; //FREE
  state.clusters.forEach((cluster) => {
    if (cluster.service_level) {
      let currentClusterPriceServel =
        ServerLevel[cluster.service_level?.toUpperCase()];
      if (currentClusterPriceServel > priceServel) {
        priceServel = currentClusterPriceServel;
      }
    }
  });
  return ServerLevel[priceServel];
}
export default {
  namespaced: true,
  state,
  mutations,
  actions,
};
