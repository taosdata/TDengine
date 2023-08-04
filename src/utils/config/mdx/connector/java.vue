<template>
  <div>
    <h2 id="add-dependency">{{ $t("docs.connector.java.step1") }}</h2>
    <el-tabs value="maven">
      <el-tab-pane name="maven" label="Maven">
        <pre v-highlight><code class="language-xml">    &lt;dependency&gt;
      &lt;groupId&gt;com.taosdata.jdbc&lt;/groupId&gt;
      &lt;artifactId&gt;taos-jdbcdriver&lt;/artifactId&gt;
      &lt;version&gt;3.1.0&lt;/version&gt;
    &lt;/dependency&gt;
</code></pre>
      </el-tab-pane>
      <el-tab-pane name="gradel" label="Gradle">
        <pre v-highlight><code class="language-groovy">dependencies {
  implementation &#39;com.taosdata.jdbc:taos-jdbcdriver:3.1.0&#39;
}
</code></pre>
      </el-tab-pane>
    </el-tabs>

    <h2 id="config">{{ $t("docs.connector.java.step2") }}</h2>
    <p>{{ $t("component.docConfig.content", [" JDBC URL "]) }}</p>
    <el-tabs value="bash">
      <el-tab-pane name="bash" label="Bash">
        <pre
          v-highlight="
            `export TDENGINE_JDBC_URL=&quot;${jdbcURL}&quot;
`
          "
        ><code class="language-bash"></code></pre>
      </el-tab-pane>
      <el-tab-pane name="cmd" label="CMD">
        <pre
          v-highlight="
            `set TDENGINE_JDBC_URL=&quot;${jdbcURL}&quot;
`
          "
        ><code class="language-bash"></code></pre>
      </el-tab-pane>
      <el-tab-pane name="powershell" label="Powershell">
        <pre
          v-highlight="
            `$env:TDENGINE_JDBC_URL=&quot;${jdbcURL}&quot;
`
          "
        ><code class="language-powershell"></code></pre>
      </el-tab-pane>
    </el-tabs>

    <p>{{ $t("component.docConfig.bottom") }}</p>
    <h2 id="connect">{{ $t("docs.connector.java.step3") }}</h2>
    <p>{{ $t("docs.connector.java.step3desc") }}</p>
    <pre v-highlight><code class="language-java">import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;


public class ConnectCloudExample {
    public static void main(String[] args) throws SQLException {
        String jdbcUrl = System.getenv(&quot;TDENGINE_JDBC_URL&quot;);
        System.out.println(jdbcUrl);
        try(Connection conn = DriverManager.getConnection(jdbcUrl)) {
            try(Statement stmt = conn.createStatement()) {
                stmt.executeQuery(&quot;select server_version()&quot;);
            }
        }
    }
}
</code></pre>
    <p>
      {{ $t("docs.connector.bottom1") }} {{ $t("docs.connector.bottom2") }}
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
    user: {
      type: String,
      default: ''
    },
    password: {
      type: String,
      default: ''
    }
  },
  data() {
    return {};
  },
  computed: {
    jdbcURL() {
      return (
        "jdbc:TAOS-RS://" +
        this.url.replace(/https?:\/\//, "") +
        "?useSSL=" +
        this.url.startsWith("https") +
        "&user=" + this.user +
        "&password=" + this.password
        // "&token=" +
        // this.token
      );
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
