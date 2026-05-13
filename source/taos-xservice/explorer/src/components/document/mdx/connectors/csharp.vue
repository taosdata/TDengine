<template>
  <div>
    <h2 id="create-project">{{ $t("docs.connector.csharp.step1") }}</h2>
    <pre
      v-highlight="
        `dotnet new console -o example
`
      "
    ><code class="language-bash"></code></pre>
    <p>{{ $t("docs.connector.csharp.step11desc") }}</p>
    <pre
      v-highlight="
        `cd example
vim example.csproj
`
      "
    ><code class="language-bash"></code></pre>
    <p>{{ $t("docs.connector.csharp.step12desc") }}</p>
    <pre
      v-highlight="
        `&lt;ItemGroup&gt;
  &lt;PackageReference Include=&quot;TDengine.Connector&quot; Version=&quot;3.0.*&quot; GeneratePathProperty=&quot;true&quot; /&gt;
&lt;/ItemGroup&gt;
&lt;Target Name=&quot;copyDLLDependency&quot; BeforeTargets=&quot;BeforeBuild&quot;&gt;
  &lt;ItemGroup&gt;
    &lt;DepDLLFiles Include=&quot;$(PkgTDengine_Connector)\\runtimes\\**\\*.*&quot; /&gt;
  &lt;/ItemGroup&gt;
  &lt;Copy SourceFiles=&quot;@(DepDLLFiles)&quot; DestinationFolder=&quot;$(OutDir)&quot; /&gt;
&lt;/Target&gt;`
      "
    ><code class="language-xml"></code></pre>
    <h2 id="config">{{ $t("docs.connector.csharp.step2") }}</h2>
    <p>{{ $t("docs.docConfig.content", [" DSN "]) }}</p>
    <p>
           <el-icon color="gold" :size="20">
        <Opportunity/>
      </el-icon>
      <span class="docker-tip">{{ $t("dockerTip", [`${url.split('//')[1]}`] )}}</span>
    </p>
    <el-tabs model-value="bash">
      <el-tab-pane name="bash" label="Bash">
        <pre
          v-highlight="
            `export TDENGINE_DSN=&quot;${DSN}&quot;
`
          "
        ><code class="language-bash"></code></pre>
      </el-tab-pane>
      <el-tab-pane name="cmd" label="CMD">
        <pre
          v-highlight="
            `set TDENGINE_DSN=&quot;${DSN}&quot;
`
          "
        ><code class="language-bash"></code></pre>
      </el-tab-pane>
      <el-tab-pane name="powershell" label="Powershell">
        <pre
          v-highlight="
            `$env:TDENGINE_DSN=&quot;${DSN}&quot;
`
          "
        ><code class="language-powershell"></code></pre>
      </el-tab-pane>
    </el-tabs>

    <h2 id="connect">{{ $t("docs.connector.csharp.step3") }}</h2>
    <p>{{ $t("docs.connector.csharp.step31desc") }}</p>
    <pre
      v-highlight="
        `&lt;Project Sdk=&quot;Microsoft.NET.Sdk&quot;&gt;

&lt;PropertyGroup&gt;
  &lt;OutputType&gt;Exe&lt;/OutputType&gt;
  &lt;TargetFramework&gt;net5.0&lt;/TargetFramework&gt;
  &lt;Nullable&gt;enable&lt;/Nullable&gt;
&lt;/PropertyGroup&gt;

&lt;ItemGroup&gt;
  &lt;PackageReference Include=&quot;TDengine.Connector&quot; Version=&quot;3.0.*&quot; GeneratePathProperty=&quot;true&quot; /&gt;
&lt;/ItemGroup&gt;
&lt;Target Name=&quot;copyDLLDependency&quot; BeforeTargets=&quot;BeforeBuild&quot;&gt;
  &lt;ItemGroup&gt;
    &lt;DepDLLFiles Include=&quot;$(PkgTDengine_Connector)\\runtimes\\**\\*.*&quot; /&gt;
  &lt;/ItemGroup&gt;
  &lt;Copy SourceFiles=&quot;@(DepDLLFiles)&quot; DestinationFolder=&quot;$(OutDir)&quot; /&gt;
&lt;/Target&gt;

&lt;/Project&gt;`
      "
    ><code class="language-xml"></code></pre>
    <p>{{ $t("docs.connector.csharp.step32desc") }}</p>
    <pre v-highlight><code class="language-C#">using System;
using TDengineWS.Impl;

namespace Cloud.Examples
{
    public class ConnectExample
    {
        static void Main(string[] args)
        {
            string dsn = Environment.GetEnvironmentVariable(&quot;TDENGINE_DSN&quot;);
            Connect(dsn);
        }

        public static void Connect(string dsn)
        {
            // get connect
            IntPtr conn = LibTaosWS.WSConnectWithDSN(dsn);
            if (conn == IntPtr.Zero)
            {
                throw new Exception($&quot;get connection failed,reason:{LibTaosWS.WSErrorStr(conn)},code:{LibTaosWS.WSErrorNo(conn)}&quot;);
            }
            else
            {
                Console.WriteLine(&quot;Establish connect success.&quot;);
            }

            // do something ...

            // close connect
            LibTaosWS.WSClose(conn);

        }
    }
}
</code></pre>
    <p>
      {{ $t("docs.connector.bottom1") }} {{ $t("docs.connector.bottom2") }}
      <a :href="`${$t('urlPart')}/${insertApi}`">{{
        `${$t('docs.connector.bottom2_1')}`
      }}</a>
      {{ $t("docs.connector.bottomand") }}
      <a :href="`${$t('urlPart')}/${selectApi}`">{{
        `${$t('docs.connector.bottom2_2')}`
      }}</a
      >{{ $t("docs.connector.bottom3end") }}
    </p>
    <p>
      {{ $t("docs.connector.bottom3") }}
      <a
        :href="`${$t('urlPart')}/${restApi}`"
        >REST API</a
      >{{ $t("docs.connector.bottom3end") }}
    </p>
  </div>
</template>

<script setup lang="ts">
import { isEn } from '@/const';
import { DocsProps } from '../utils'

const props = defineProps<DocsProps>()

const DSN = computed(() => {
  // "ws://root:taosdata@127.0.0.1:6041/test";
  const uri = props.url.replace(/https?:\/\//, "");
  return `ws://${props.user}:${props.password}@${uri}`;
})

const restApi = computed(() => isEn.value ? 'tdengine-reference/client-libraries/rest-api/' : 'reference/connector/rest-api/');
const insertApi = computed(() => isEn.value ? 'developer-guide/running-sql-statements/#insert-data' : 'develop/sql/#插入数据');
const selectApi = computed(() => isEn.value ? 'developer-guide/running-sql-statements/#query-data' : 'develop/sql/#查询数据');
 
</script>
