<template>
  <div>
    <p>
      {{ $t("docs.virtual.grafana.topdesc")
      }}<a href="https://www.grafana.com/">Grafana</a
      >{{ $t("docs.virtual.grafana.topdesc1") }}
    </p>
    <p>
      {{ $t("docs.virtual.grafana.topdesc2")
      }}<a
        href="https://github.com/taosdata/grafanaplugin/blob/master/README.md"
        >GitHub</a
      >.
    </p>
    <h2 id="install-grafana">{{ $t("docs.virtual.grafana.step1") }}</h2>
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
        `export TDENGINE_TOKEN=&quot;${token}&quot;
export TDENGINE_URL=&quot;${url}&quot;
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
    <p>
      <img
        src="./assets/grafana/verifying-tdengine-datasource.webp"
        alt="Verify TDengine data source"
      />
    </p>
    <h2 id="use-grafana">{{ $t("docs.virtual.grafana.step4") }}</h2>
    <p>{{ $t("docs.virtual.grafana.step4desc") }}</p>
    <p>
      {{ $t("docs.virtual.grafana.step4desc1")
      }}<a
        :href="`https://docs.${urlPart}.com/third-party/grafana#create-dashboard`"
        >{{ $t("docs.virtual.grafana.step4desc2") }}</a
      >{{ $t("docs.virtual.grafana.step4desc3") }}
    </p>
  </div>
</template>

<script>
import { IsAliyun } from "@/const";
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
  computed: {
    urlPart() {
      return navigator.language.includes('en') ?"tdengine": "taosdata";
    },
  },
};
</script>
