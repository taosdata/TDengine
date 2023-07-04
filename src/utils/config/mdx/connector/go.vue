<template>
  <div>
    <h2 id="initialize-module">{{ $t("docs.connector.go.step1") }}</h2>
    <p>{{ $t("docs.connector.go.step1desc") }}</p>
    <pre v-highlight><code>go mod init tdengine.com/example</code></pre>
    <h2 id="add-dependency">{{ $t("docs.connector.go.step2") }}</h2>
    <p>{{ $t("docs.connector.go.step2desc") }}</p>
    <pre v-highlight><code class="language-go-mod">module tdengine.com/example

go 1.17

require github.com/taosdata/driver-go/v3 latest
</code></pre>
    <doc-config
      :url="endpoint"
      :need-token="false"
      :url-key="'TDENGINE_GO_DSN'"
      :url-des="$t('component.docConfig.dsn')"
    ></doc-config>

    <h2 id="connect">{{ $t("docs.connector.go.step4") }}</h2>
    <p>{{ $t("docs.connector.go.step4desc") }}</p>
    <pre v-highlight><code class="language-go">package main

import (
  &quot;database/sql&quot;
  &quot;fmt&quot;
  &quot;os&quot;

  _ &quot;github.com/taosdata/driver-go/v3/taosRestful&quot;
)

func main() {
  dsn := os.Getenv(&quot;TDENGINE_GO_DSN&quot;)
 
  taos, err := sql.Open(&quot;taosRestful&quot;, dsn)
  if err != nil {
      fmt.Println(err)
      return
  }
  defer taos.Close()
  rows, err := taos.Query(&quot;show databases&quot;)
  if err != nil {
      fmt.Println(err)
      return
  }
  rows.Close()
  fmt.Println(&quot;connect success&quot;)
}
</code></pre>
    <p>{{ $t("docs.connector.go.step4desc1") }}</p>
    <pre v-highlight><code>go mod tidy
</code></pre>
    <p>{{ $t("docs.connector.go.step4desc2") }}</p>
    <pre v-highlight><code>go run main.go
</code></pre>
    <p>
      {{ $t("docs.connector.bottom2") }}
      <a :href="`https://docs.${urlPart}.com/develop/insert-data/`">{{
        `https://docs.${urlPart}.com/develop/insert-data/`
      }}</a>
      {{ $t("docs.connector.bottomand") }}
       <a :href="`https://docs.${urlPart}.com/develop/query-data/`">{{
        `https://docs.${urlPart}.com/develop/query-data/`
      }}</a
      >{{ $t("docs.connector.bottom3end") }}
    </p>
    <p>
      {{ $t("docs.connector.bottom3") }}
      <a
        :href="`https://docs.${urlPart}.com/${restapi}/rest-api/`"
        >REST API</a
      >{{ $t("docs.connector.bottom3end") }}
    </p>
  </div>
</template>

<script>
import DocConfig from "@/components/DocConfig/index.vue";
export default {
  components: { DocConfig },
  props: {
    token: {
      type: String,
      default: "",
    },
    url: {
      type: String,
      default: "",
    },
    user: {
      type: String,
      default: ''
    },
    password: {
      type: String,
      default: ''
    }
  },
  computed: {
    endpoint() {
      // root:taosdata@http(localhost:6041)/test?readBufferSize=52428800
      let uri = this.url.replace(/(https?):\/\//, "$1(");
      // const tokenStr = this.token;
      if (this.url.startsWith("https") && uri.indexOf(":") < 0) {
        uri += ":443";
      }
      return `${this.user}:${this.password}@${uri})`;
    },
    urlPart() {
      return navigator.language.includes('en') ?"tdengine": "taosdata";
    },
    restapi(){
      return navigator.language.includes('en') ?"reference": "connector";
    }
  },
};
</script>
