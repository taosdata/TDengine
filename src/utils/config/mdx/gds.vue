<template>
  <div>
    <p>
      {{ $t("docs.virtual.gds.topdesc") }}
      <a href="https://datastudio.google.com/data?search=TDengine">{{
        $t("docs.virtual.gds.topconnector")
      }}</a
      >{{ $t("docs.virtual.gds.topdesc1") }}
    </p>
    <p>
      {{ $t("docs.virtual.gds.topdesc2") }}
      <a href="https://github.com/taosdata/gds-connector/blob/master/README.md"
        >GitHub</a
      >
      {{ $t("docs.virtual.gds.topdesc3") }}
    </p>
    <h2 id="choose-data-source">{{ $t("docs.virtual.gds.step1") }}</h2>
    <p>
      {{ $t("docs.virtual.gds.step1desc")
      }}<a href="https://datastudio.google.com/data?search=TDengine">{{
        $t("docs.virtual.gds.step1desc1")
      }}</a>
      {{ $t("docs.virtual.gds.step1desc2") }}
    </p>
    <p v-if="!isOEM">
      <img
        src="./assets/gds/gds_data_source.webp"
        alt="Data Studio Data Source Selection"
      />
    </p>
    <h2 id="connector-configuration">{{ $t("docs.virtual.gds.step2") }}</h2>
    <h3 id="mandatory-config">{{ $t("docs.virtual.gds.step21") }}</h3>
    <h4 id="url">{{ $t("docs.virtual.gds.step21desc") }}</h4>
    <pre v-highlight="`${cloud_url}`"><code class="language-bash"></code></pre>
    <h4 id="tdengine-cloud-token">{{ $t("docs.virtual.gds.step211") }}</h4>
    <pre
      v-highlight="`${cloud_token}`"
    ><code class="language-bash"></code></pre>
    <h4 id="database">{{ $t("docs.virtual.gds.step212") }}</h4>
    <p>{{ $t("docs.virtual.gds.step212desc") }}</p>
    <h4 id="table">{{ $t("docs.virtual.gds.step213") }}</h4>
    <p>{{ $t("docs.virtual.gds.step213desc") }}</p>
    <p>
      <strong>{{ $t("docs.virtual.gds.step213desc1") }}</strong
      >{{ $t("docs.virtual.gds.step213desc2") }}
    </p>
    <h3 id="optional-config">{{ $t("docs.virtual.gds.step22") }}</h3>
    <h4 id="query-range-start-date--end-date">
      {{ $t("docs.virtual.gds.step221") }}
    </h4>
    <p>{{ $t("docs.virtual.gds.step221desc") }}</p>
    <pre
      v-highlight="
        `2022-05-12 18:24:15
`
      "
    ><code class="language-bash"></code></pre>
    <p>{{ $t("docs.virtual.gds.step221desc1") }}</p>
    <p>{{ $t("docs.virtual.gds.step221desc2") }}</p>
    <pre
      v-highlight
    ><code class="language-SQL">-- select * from table_name where ts &gt;= start_date and ts &lt;= end_date
select * from test.demo where ts &gt;= &#39;2022-05-10 18:24:15&#39; and ts&lt;=&#39;2022-05-12 18:24:15&#39;
</code></pre>
    <p>{{ $t("docs.virtual.gds.step221desc3") }}</p>
    <p v-if="!isOEM">
      <img
        src="./assets/gds/gds_cloud_login.webp"
        alt="TDengine  Config Page"
      />
    </p>
    <p>{{ $t("docs.virtual.gds.step221desc4") }}</p>
    <h2 id="create-report-or-dashboard">{{ $t("docs.virtual.gds.step3") }}</h2>
    <p>{{ $t("docs.virtual.gds.step3desc") }}</p>
    <p v-if="!isOEM">
      {{ $t("docs.virtual.gds.step3desc1")
      }}&nbsp;<a :href="`${$t('urlPart')}/third-party/bi/looker/`">{{
        $t("docs.virtual.gds.step3desc2")
      }}</a>
      {{ $t("docs.virtual.gds.step3desc3") }}
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
  data(){
    return {
      isOEM:
        process.env.VUE_APP_CUS_NAME &&
        process.env.VUE_APP_CUS_NAME !== "TDengine",
    }
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
    urlPart() {
      return this.$i18n.locale.includes('en') ?"tdengine": "taosdata";
    },
  },
};
</script>
