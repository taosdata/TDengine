<template>
  <div>
    <p>{{ $t("docs.party.prometheus.totaldesc1") }}</p>
    <p>{{ $t("docs.party.prometheus.totaldesc2") }}</p>
    <h2 id="prerequisites">{{ $t("docs.party.prometheus.step1") }}</h2>
    <p>{{ $t("docs.party.prometheus.step1desc") }}</p>
    <h2 id="install-prometheus">{{ $t("docs.party.prometheus.step2") }}</h2>
    <p>{{ $t("docs.party.prometheus.step2desc") }}</p>
    <ol>
      <li>
        {{ $t("docs.party.prometheus.step21") }}
        <pre
          v-highlight
        ><code>wget https://github.com/prometheus/prometheus/releases/download/v2.37.0/prometheus-2.37.0.linux-amd64.tar.gz
</code></pre>
      </li>
      <li>
        {{ $t("docs.party.prometheus.step22") }}
        <pre
          v-highlight
        ><code>tar xvfz prometheus-*.tar.gz &amp;&amp; mv prometheus-2.37.0.linux-amd64 prometheus
</code></pre>
      </li>
      <li>
        {{ $t("docs.party.prometheus.step23") }}
        <pre v-highlight><code>cd prometheus
</code></pre>
      </li>
    </ol>
    <p>
      {{ $t("docs.party.prometheus.step2end") }}
      <a href="https://prometheus.io/docs/prometheus/latest/installation/">{{
        $t("docs.party.prometheus.step2doc")
      }}</a
      >{{ $t("docs.connector.bottom3end") }}
    </p>
    <h2 id="configure-prometheus">{{ $t("docs.party.prometheus.step3") }}</h2>
    <p>{{ $t("docs.party.prometheus.step3desc") }}</p>
    <pre
      v-highlight="
        `remote_write:
  - url: &quot;${cloud_url}/prometheus/v1/remote_write/prometheus_data?token=${cloud_token}&quot;

remote_read:
  - url: &quot;${cloud_url}/prometheus/v1/remote_read/prometheus_data?token=${cloud_token}&quot;
    remote_timeout: 10s
    read_recent: true
`
      "
    ><code class="language-yaml"></code></pre>
    <p>{{ $t("docs.party.prometheus.step3desc1") }}</p>
    <h2 id="start-prometheus">{{ $t("docs.party.prometheus.step4") }}</h2>
    <pre v-highlight><code>./prometheus --config.file prometheus.yml
</code></pre>
    <p>
      {{ $t("docs.party.prometheus.step4desc")
      }}<a href="http://localhost:9090">http://localhost:9090</a
      >{{ $t("docs.party.prometheus.step4desc1") }}
    </p>
    <h2 id="verify-remote-write">{{ $t("docs.party.prometheus.step5") }}</h2>
    <p>{{ $t("docs.party.prometheus.step5desc") }}</p>
    <p>
      <img
        src="./assets/prometheus/prometheus_data.webp"
        alt="TDengine prometheus remote_write result"
      />
    </p>
    <ul>
      <li>{{ $t("docs.party.prometheus.step5desc1") }}</li>
    </ul>
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
    return {};
  },
  computed: {
    jdbcURL() {
      return (
        "jdbc:TAOS-RS://" +
        this.url.replace(/https?:\/\//, "") +
        "?usessl=" +
        this.url.startsWith("https") +
        "&token=" +
        this.token
      );
    },
    goDSN() {
      return (
        (this.url.startsWith("https") ? "https" : "http") +
        "(" +
        this.url.replace(/https?:\/\//, "") +
        ")/?token=" +
        this.token
      );
    },
    DSN() {
      return this.url + "?token=" + this.token;
    },
    cloud_url() {
      return this.url;
    },
    cloud_token() {
      return this.token;
    },
  },
};
</script>
