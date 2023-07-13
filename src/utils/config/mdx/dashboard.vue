<template>
  <div>
    <p v-html="$t('docs.dashboard.dashboarddesc')"></p>

    <h2 id="install-grafana">{{ $t("docs.dashboard.step1") }}</h2>
    <el-tabs value="tab1">
      <el-tab-pane name="tab1" :label="$t('docs.dashboard.tab1')">
        <pre><code class="language-bash">sudo apt-get install -y apt-transport-https
sudo apt-get install -y software-properties-common wget
wget -q -O - https://packages.grafana.com/gpg.key |\
  sudo apt-key add -
echo "deb https://packages.grafana.com/oss/deb stable main" |\
  sudo tee -a /etc/apt/sources.list.d/grafana.list
sudo apt-get update
sudo apt-get install grafana
</code></pre>
      </el-tab-pane>
      <el-tab-pane name="tab2" :label="$t('docs.dashboard.tab2')">
        <pre><code class="language-bash">sudo tee /etc/yum.repos.d/grafana.repo &lt;&lt; EOF
[grafana]
name=grafana
baseurl=https://packages.grafana.com/oss/rpm
repo_gpgcheck=1
enabled=1
gpgcheck=1
gpgkey=https://packages.grafana.com/gpg.key
sslverify=1
sslcacert=/etc/pki/tls/certs/ca-bundle.crt
EOF
sudo yum install grafana
</code></pre>
        <p>{{ $t("docs.dashboard.tab2sub") }}</p>
        <pre>
<code class="language-bash">wget https://dl.grafana.com/oss/release/grafana-7.5.11-1.x86_64.rpm
sudo yum install grafana-7.5.11-1.x86_64.rpm
# or
sudo yum install \
  https://dl.grafana.com/oss/release/grafana-7.5.11-1.x86_64.rpm</code>
</pre>
      </el-tab-pane>
      <el-tab-pane name="tb1" :label="$t('docs.dashboard.tab3')">
        <p>{{ $t("docs.dashboard.plugin1") }}</p>
        <pre>
<code class="language-bash">get_latest_release() {
  curl --silent "https://api.github.com/repos/taosdata/grafanaplugin/releases/latest" |
    grep '"tag_name":' |
    sed -E 's/.*"v([^"]+)".*/\1/'
}
TDENGINE_PLUGIN_VERSION=$(get_latest_release)
sudo grafana-cli \
  --pluginUrl https://github.com/taosdata/grafanaplugin/releases/download/v$TDENGINE_PLUGIN_VERSION/tdengine-datasource-$TDENGINE_PLUGIN_VERSION.zip \
  plugins install tdengine-datasource
        </code>
      </pre>
      </el-tab-pane>
       <el-tab-pane name="tb2" :label="$t('docs.dashboard.tab4')">
        <p v-html="$t('docs.dashboard.plugin2')"></p>
        <pre>
<code class="language-bash">wget https://github.com/taosdata/grafanaplugin/releases/latest/download/TDinsight.sh
chmod +x TDinsight.sh
./TDinsight.sh</code>
        </pre>
        <p v-html="$t('docs.dashboard.pluginsub2')"></p>
      </el-tab-pane>
    </el-tabs>

    <!-- <h2 id="install-tdengine-plugin">{{ $t("docs.dashboard.step2") }}</h2> -->
    <!-- <el-tabs value="tb1"> -->
      <!-- <el-tab-pane name="tb1" :label="$t('docs.dashboard.pluginname1')">
         <p>{{$t('docs.dashboard.plugin1')}}</p>
      <pre  >
<code class="language-bash">get_latest_release() {
  curl --silent "https://api.github.com/repos/taosdata/grafanaplugin/releases/latest" |
    grep '"tag_name":' |
    sed -E 's/.*"v([^"]+)".*/\1/'
}
TDENGINE_PLUGIN_VERSION=$(get_latest_release)
sudo grafana-cli \
  --pluginUrl https://github.com/taosdata/grafanaplugin/releases/download/v$TDENGINE_PLUGIN_VERSION/tdengine-datasource-$TDENGINE_PLUGIN_VERSION.zip \
  plugins install tdengine-datasource
        </code>
      </pre>
      </el-tab-pane> -->

      <!-- <el-tab-pane name="tb2" :label="$t('docs.dashboard.pluginname2')">
        <p v-html="$t('docs.dashboard.plugin2')"></p>
        <pre>
<code class="language-bash">wget https://github.com/taosdata/grafanaplugin/releases/latest/download/TDinsight.sh
chmod +x TDinsight.sh
./TDinsight.sh</code>
        </pre>
        <p v-html="$t('docs.dashboard.pluginsub2')"></p>
      </el-tab-pane> -->
    <!-- </el-tabs> -->

    <h2 id="start-grafana-server">{{ $t("docs.dashboard.step3") }}</h2>
    <pre>
<code class="language-bash">sudo systemctl start grafana-server
sudo systemctl enable grafana-server</code>
    </pre>
    <h2 id="login-in-grafana">{{ $t("docs.dashboard.step4") }}</h2>
    <p v-html="$t('docs.dashboard.logingrafana')"></p>
    <h2 id="add-grafana-dbsource">{{ $t("docs.dashboard.step5") }}</h2>
    <p v-html="$t('docs.dashboard.nav')"></p>
    <p>
      <img
        src="./assets/dashboard/configuration.webp"
        alt="TDengine Database TDinsight 添加数据源按钮"
      />
    </p>
    <p v-html="$t('docs.dashboard.subsearch')"></p>
    <p>
      <img
        src="./assets/dashboard/add.webp"
        alt="TDengine Database TDinsight 添加数据源按钮"
      />
    </p>
    <p>{{ $t("docs.dashboard.settingtd") }}</p>
    <p>
      <img
        src="./assets/dashboard/howto-add-datasource.webp"
        alt="TDengine Database TDinsight 添加数据源按钮"
      />
    </p>
    <p>{{ $t("docs.dashboard.savetest") }}</p>
    <p>
      <img
        src="./assets/dashboard/done.webp"
        alt="TDengine Database TDinsight 添加数据源按钮"
      />
    </p>
    <!-- <h2 id="import-dashboard">{{ $t("docs.dashboard.step6") }}</h2> -->

    <p v-html="$t('docs.dashboard.import')"></p>
    <p>
      <img
        src="./assets/dashboard/import.webp"
        alt="TDengine Database TDinsight 添加数据源按钮"
      />
    </p>
    <p v-html="$t('docs.dashboard.cont1')"></p>
    <p v-html="$t('docs.dashboard.cont2')"></p>
    <p>
      <img
        src="./assets/dashboard/search.webp"
        alt="TDengine Database TDinsight 添加数据源按钮"
      />
    </p>
    <p v-html="$t('docs.dashboard.cont3')"></p>
    <p>
      <img
        src="./assets/dashboard/keeper.webp"
        alt="TDengine Database TDinsight 添加数据源按钮"
      />
    </p>
    <p>{{ $t("docs.dashboard.cont4") }}</p>
  </div>
</template>

<script>
import Prism from "prismjs";
import "prismjs/themes/prism.css";
import "prismjs/components/prism-bash";
export default {
  props: {
    token: {
      type: String,
      default: "",
    },
    url: {
      type: String,
      default: "",
    },
  },
  data() {
    return {
      isOEM:
        process.env.VUE_APP_CUS_NAME &&
        process.env.VUE_APP_CUS_NAME !== "TDengine",
    };
  },
  computed: {
    urlPart() {
      return navigator.language.includes("en") ? "tdengine" : "taosdata";
    },
    replaceTDENGINE() {
      return this.isOEM ? "" : "TDENGINE";
    },
  },
  mounted() {
    Prism.highlightAll();
  },
};
</script>
<style lang="scss" scoped>
img{
  width: 894px;
  height:229px;
  object-fit: fill;
}
</style>
