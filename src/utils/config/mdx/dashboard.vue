<template>
  <div>
    <p v-html="$t('docs.dashboard.dashboarddesc')"></p>

    <h2 id="install-grafana">{{ $t("docs.dashboard.step1") }}</h2>
    <el-tabs value="tab1">
      <el-tab-pane name="tab1" :label="$t('docs.dashboard.tab1')">
        <pre
          v-highlight
        ><code class="language-bash">sudo apt-get install -y apt-transport-https
sudo apt-get install -y software-properties-common wget
wget -q -O - https://packages.grafana.com/gpg.key |\
  sudo apt-key add -
echo "deb https://packages.grafana.com/oss/deb stable main" |\
  sudo tee -a /etc/apt/sources.list.d/grafana.list
sudo apt-get update
sudo apt-get install grafana
</code></pre>
      </el-tab-pane>
      <el-tab-pane name="tab2" :label="$t('docs.dashboard.tab1')">
        <pre v-highlight><code class="language-bash">sudo tee /etc/yum.repos.d/grafana.repo &lt;&lt; EOF
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
      </el-tab-pane>
    </el-tabs>
    <p>
      {{ $t("docs.virtual.grafana.step1desc")
      }}<a href="https://grafana.com/grafana/download"
        >https://grafana.com/grafana/download</a
      >.
    </p>
    <h2 id="install-tdengine-plugin">{{ $t("docs.virtual.grafana.step2") }}</h2>
    <p>{{ $t("docs.virtual.grafana.step2desc") }}</p>
    <pre
      v-highlight="
        `export ${replaceTDENGINE}_TOKEN=&quot;${token}&quot;
export ${replaceTDENGINE}_URL=&quot;${url}&quot;
`
      "
    ><code class="language-bash"></code></pre>
    <p>{{ $t("docs.virtual.grafana.step2desc1") }}</p>
    <pre
      v-highlight="
        `bash -c &quot;$(curl -fsSL https://raw.githubusercontent.com/taosdata/grafanaplugin/master/install.sh)&quot;
`
      "
    ><code class="language-bash"></code></pre>
    <p>{{ $t("docs.virtual.grafana.step2desc2") }}</p>
    <pre
      v-highlight="
        `sudo systemctl restart grafana-server.service
`
      "
    ><code class="language-bash"></code></pre>
    <h2 id="verify-plugin">{{ $t("docs.virtual.grafana.step3") }}</h2>
    <p>{{ $t("docs.virtual.grafana.step3desc") }}</p>
    <p v-if="!isOEM">
      <img
        src="./assets/grafana/verifying-tdengine-datasource.webp"
        alt="Verify TDengine data source"
      />
    </p>
    <h2 id="use-grafana">{{ $t("docs.virtual.grafana.step4") }}</h2>
    <p>{{ $t("docs.virtual.grafana.step4desc") }}</p>
    <p v-if="!isOEM">
      {{ $t("docs.virtual.grafana.step4desc1")
      }}<a
        :href="`https://docs.${urlPart}.com/third-party/grafana#create-dashboard`"
        >{{ $t("docs.virtual.grafana.step4desc2") }}</a
      >{{ $t("docs.virtual.grafana.step4desc3") }}
    </p>
  </div>
</template>

<script>
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
};
</script>
