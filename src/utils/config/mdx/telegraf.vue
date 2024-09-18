<template>
  <div>
    <p>{{ $t("docs.party.telegraf.totaldesc1") }}</p>
    <p>{{ $t("docs.party.telegraf.totaldesc2") }}</p>
    <h2 id="prerequisites">{{ $t("docs.party.telegraf.step1") }}</h2>
    <p>{{ $t("docs.party.telegraf.step1desc") }}</p>
    <h2 id="install-telegraf">{{ $t("docs.party.telegraf.step2") }}</h2>
    <p>{{ $t("docs.party.telegraf.step2desc") }}</p>
    <pre
      v-highlight="
        `wget -q https://repos.influxdata.com/influxdb.key
echo &#39;23a1c8836f0afc5ed24e0486339d7cc8f6790b83886c4c96995b88a061c5bb5d influxdb.key&#39; | sha256sum -c &amp;&amp; cat influxdb.key | gpg --dearmor | sudo tee /etc/apt/trusted.gpg.d/influxdb.gpg } /dev/null
echo &#39;deb [signed-by=/etc/apt/trusted.gpg.d/influxdb.gpg] https://repos.influxdata.com/debian stable main&#39; | sudo tee /etc/apt/sources.list.d/influxdata.list
sudo apt-get update &amp;&amp; sudo apt-get install telegraf
`
      "
    ><code class="language-bash"></code></pre>
    <p>{{ $t("docs.party.telegraf.step2desc1") }}</p>
    <pre
      v-highlight="
        `sudo systemctl stop telegraf
`
      "
    ><code class="language-bash"></code></pre>
    <p>
      {{ $t("docs.party.telegraf.step2end")
      }}<a href="https://docs.influxdata.com/telegraf/v1.23/install/">
        {{ $t("docs.party.telegraf.step2doc") }}</a
      >{{ $t("docs.connector.bottom3end") }}
    </p>
    <h2 id="configure">{{ $t("docs.party.telegraf.step3") }}</h2>
    <p>{{ $t("docs.party.telegraf.step3desc") }}</p>
    <pre
      v-highlight="
        `export TDENGINE_URL=&quot;${url}&quot;
export TDENGINE_TOKEN=&quot;${token}&quot;
`
      "
    ><code class="language-bash"></code></pre>
    <p>{{ $t("docs.party.telegraf.step3desc1") }}</p>
    <pre
      v-highlight="
        `telegraf --sample-config --input-filter cpu:mem --output-filter http } telegraf.conf
`
      "
    ><code class="language-bash"></code></pre>
    <p>{{ $t("docs.party.telegraf.step3desc2") }}</p>
    <pre v-highlight><code class="language-toml">[[outputs.http]]
  url = &quot;${TDENGINE_URL}/influxdb/v1/write?db=telegraf&amp;token=${TDENGINE_TOKEN}&quot;
  method = &quot;POST&quot;
  timeout = &quot;5s&quot;
  data_format = &quot;influx&quot;
  influx_max_line_bytes = 250
</code></pre>
    <p>{{ $t("docs.party.telegraf.step3desc3") }}</p>
    <h2 id="start-telegraf">{{ $t("docs.party.telegraf.step4") }}</h2>
    <p>{{ $t("docs.party.telegraf.step4desc") }}</p>
    <pre
      v-highlight="
        `telegraf --config telegraf.conf
`
      "
    ><code class="language-bash"></code></pre>
    <h2 id="verify">{{ $t("docs.party.telegraf.step5") }}</h2>
    <ul>
      <li>{{ $t("docs.party.telegraf.step5desc") }}</li>
    </ul>
    <pre v-highlight><code class="language-sql">show databases;
</code></pre>
    <p>
      <img
        src="./assets/telegraf/telegraf-show-databases.webp"
        alt="TDengine show telegraf databases"
      />
    </p>
    <p>{{ $t("docs.party.telegraf.step5desc1") }}</p>
    <pre v-highlight><code class="language-sql">show telegraf.stables;
</code></pre>
    <p>
      <img
        src="./assets/telegraf/telegraf-show-stables.webp"
        alt="TDengine  show telegraf stables"
      />
    </p>
    <ul>
      <li>
        {{ $t("docs.party.telegraf.step5desc2") }}
        <a href="https://docs.influxdata.com/telegraf/v1.22/plugins/" target="_blank">
          {{ $t("docs.party.telegraf.step5desc2input") }}</a
        >
        {{ $t("docs.party.telegraf.step5desc2insert") }}
        <a
          href="https://docs.influxdata.com/telegraf/v1.24/data_formats/input/"
          target="_blank"
        >
          {{ $t("docs.party.telegraf.step5desc2format") }}</a
        >
        {{ $t("docs.party.telegraf.step5desc2end") }}
      </li>
      <li>
        {{ $t("docs.party.telegraf.step5desc3") }}
        <a :href="schemelessUrl" target="_blank">{{
          $t("docs.party.telegraf.step5desc3end")
        }}</a>
      </li>
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
    return {
      nativeLanguage:this.$i18n.locale,
      zhDomain:'https://docs.taosdata.com',
      enDomain:'https://docs.tdengine.com'
    };
  },
  computed: {
    schemelessUrl(){
      return (
        (this.$t('urlPart'))+'/develop/schemaless'
      )
    },
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
