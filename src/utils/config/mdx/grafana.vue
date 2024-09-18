<template>
  <div>
    <p>
      {{ $t("docs.virtual.grafana.topdesc")
      }}<a href="https://www.grafana.com/"> Grafana</a
      >{{ $t("docs.virtual.grafana.topdesc1") }}<a
        href="https://github.com/taosdata/grafanaplugin/blob/master/README.md"
        >GitHub</a
      >.
    </p>
    <!-- <p>
      {{ $t("docs.virtual.grafana.topdesc2")
      }}<a
        href="https://github.com/taosdata/grafanaplugin/blob/master/README.md"
        >GitHub</a
      >.
    </p> -->
  
    <h2 id="install-grafana">{{ $t("docs.virtual.grafana.step1") }}</h2>
    <p>
        {{ $t("docs.virtual.grafana.step1desc")
      }}(<a href="https://grafana.com/grafana/download"
        >https://grafana.com/grafana/download</a
      >).
    </p>
    <h2 id="install-tdengine-plugin">{{ $t("docs.virtual.grafana.step2") }}</h2>
    <el-tabs v-model="activeTab">
    <el-tab-pane name="plugins" label="Plugins">
      <!-- <p>{{$t('docs.virtual.grafana.pluginsdesc')}}</p> -->
      <p v-html="$t('docs.virtual.grafana.plugin1desc')"></p>
      <p v-html="$t('docs.virtual.grafana.plugin2desc')"></p>
      <p v-html="$t('docs.virtual.grafana.plugin3desc')"></p>
      <p v-html="$t('docs.virtual.grafana.plugin4desc')"></p>
    </el-tab-pane>
    <el-tab-pane name="script" label="Script"><p>{{ $t('docs.virtual.grafana.script1') }}</p>
    <pre
          v-highlight="
            `bash -c &quot;$(curl -fsSL https://raw.githubusercontent.com/taosdata/grafanaplugin/master/install.sh)&quot;
`
          "
        ><code class="language-bash"></code></pre>
        <p>{{ $t('docs.virtual.grafana.script2') }}</p>
        <pre
          v-highlight="
            `sudo systemctl restart grafana-server.service
`
          "
        ><code class="language-bash"></code></pre>

    </el-tab-pane>
    
    </el-tabs>
    <!-- <p>{{ $t("docs.virtual.grafana.step2desc") }}</p>
    <pre
      v-highlight="
    
        `export ${replaceTDENGINE}_TOKEN=&quot;${token}&quot;
export ${replaceTDENGINE}_URL=&quot;${url}&quot;
`
      "
    ><code class="language-bash"></code></pre> -->
    <!-- <p>{{ $t("docs.virtual.grafana.step2desc1") }}</p>
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
    ><code class="language-bash"></code></pre> -->
    <h2 id="verify-plugin">{{ $t("docs.virtual.grafana.step3") }}</h2>
    <p>{{ $t('docs.virtual.grafana.step3desc') }}</p>
    <p style="display:flex;align-items: baseline;margin-bottom: 0px;">
        <span style="width:100px;">{{ $t('docs.virtual.grafana.step3desc1') }}</span>
      <pre
          v-highlight="
       
            `${url}
`
          "
        ><code class="language-bash"></code></pre>
    </p>
    <p style="display:flex;align-items: baseline;margin-bottom: 0px;">
        <span style="width:100px;">{{ $t('docs.virtual.grafana.step3desc2') }}</span>
      <pre
          v-highlight="
            `${username}
`
          "
        ><code class="language-bash"></code></pre>
    </p>
    <!-- <p style="display:flex;align-items: baseline;margin-bottom: 0px;">
        <span style="width:100px;">{{ $t('docs.virtual.grafana.ste3pwd') }}</span>
      <pre
          v-highlight="
            `${pwd}
`
          "
        ><code class="language-bash"></code></pre>
    </p> -->
     
   
  
  
    <p v-html="$t('docs.virtual.grafana.step3desc3')"></p>
    <!-- <p>{{ $t("docs.virtual.grafana.step3desc") }}</p>
    <p v-if="!isOEM">
      <img
        src="./assets/grafana/verifying-tdengine-datasource.webp"
        alt="Verify TDengine data source"
      />
    </p> -->
    <h2 id="use-grafana">{{ $t("docs.virtual.grafana.step4") }}</h2>
    <p>{{ $t("docs.virtual.grafana.step4desc") }}
    <span v-if="!isOEM">
      {{ $t("docs.virtual.grafana.step4desc1")
      }}
      <a
        :href="`${$t('urlPart')}/third-party/visual/grafana/#创建-dashboard`"
        >{{ $t("docs.virtual.grafana.step4desc2") }}</a
      >   {{ $t("docs.virtual.grafana.step4desc3") }}</span>
    </p>
    <!-- <p v-if="!isOEM">
      {{ $t("docs.virtual.grafana.step4desc1")
      }}<a
        :href="`${$t('urlPart')}/third-party/grafana#create-dashboard`"
        >{{ $t("docs.virtual.grafana.step4desc2") }}</a
      >{{ $t("docs.virtual.grafana.step4desc3") }}
    </p> -->
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
  data(){
    return {
      username:localStorage.getItem('username'),
      pwd:localStorage.getItem('pwd'),
      activeTab:'plugins',
      isOEM:
        process.env.VUE_APP_CUS_NAME &&
        process.env.VUE_APP_CUS_NAME !== "TDengine",
    }
  },
  computed: {
    urlPart() {
      return this.$i18n.locale.includes('en') ?"tdengine": "taosdata";
    },
    replaceTDENGINE(){
      return this.isOEM?'':'TDENGINE'
    }
  },
};
</script>
<style>
.pre-code{
  width:100%;
}
</style>
