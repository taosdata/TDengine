<template>
  <div>
    <h2 id="create-project">{{ t('connector.csharp.step1') }}</h2>
    <pre
      v-highlight="
        `dotnet new console -o example
`
      "
    ><code class="language-bash"></code></pre>
    <p>{{ t('connector.csharp.step11desc') }}</p>
    <pre
      v-highlight="
        `cd example
vim example.csproj
`
      "
    ><code class="language-bash"></code></pre>
    <p>{{ t('connector.csharp.step12desc') }}</p>
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
    <doc-config
      need-token
      :url="endpoint"
      :token="instance.token"
      url-key="TDENGINE_CLOUD_ENDPOINT"
      :url-des="t('docsConfig.endpoint')"
    ></doc-config>
    <h2 id="connect">{{ t('connector.csharp.step3') }}</h2>
    <p>{{ t('connector.csharp.step31desc') }}</p>
    <pre
      v-highlight="
        `&lt;Project Sdk=&quot;Microsoft.NET.Sdk&quot;&gt;

&lt;PropertyGroup&gt;
  &lt;OutputType&gt;Exe&lt;/OutputType&gt;
  &lt;TargetFramework&gt;net6.0&lt;/TargetFramework&gt;
  &lt;Nullable&gt;enable&lt;/Nullable&gt;
&lt;/PropertyGroup&gt;

&lt;ItemGroup&gt;
  &lt;PackageReference Include=&quot;TDengine.Connector&quot; Version=&quot;3.1.*&quot; GeneratePathProperty=&quot;true&quot; /&gt;
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
    <p>{{ t('connector.csharp.step32desc') }}</p>
    <pre v-highlight><code class="language-C#">using System;
      using System.Text;
using TDengine.Driver;
using TDengine.Driver.Client;

namespace Cloud.Examples
{
    public class ConnectExample
    {
        static void Main(string[] args)
        {
           var cloudEndPoint = Environment.GetEnvironmentVariable("TDENGINE_CLOUD_ENDPOINT");
           var cloudToken = Environment.GetEnvironmentVariable("TDENGINE_CLOUD_TOKEN");
           var connectionString = $"protocol=WebSocket;host={cloudEndPoint};port=443;useSSL=true;token={cloudToken};";
           // Connect to TDengine server using WebSocket
           var builder = new ConnectionStringBuilder(connectionString);
          try
          {
             // Open connection with using block, it will close the connection automatically
             using (var client = DbDriver.Open(builder))
             {
               Console.WriteLine("Connected to " + builder.ToString() + " successfully.");
             }
          }
          catch (TDengineError e)
          {
             // handle TDengine error
             Console.WriteLine("Failed to connect to " + builder.ToString() + "; ErrCode:" + e.Code +
                                              "; ErrMessage: " + e.Error);
             throw;
          }
          catch (Exception e)
          {
             // handle other exceptions
             Console.WriteLine("Failed to connect to " + builder.ToString() + "; Err:" + e.Message);
             throw;
          }
        }
    }
}
</code></pre>
    <p>
      {{ t('connector.bottom1') }} {{ t('connector.bottom2') }}
      <a :href="`${docs.urlPrefix}/programming/insert/`">{{ t('common.insert') }}</a>
      {{ t('connector.bottomand') }}
      <a :href="`${docs.urlPrefix}/programming/query/`">{{ t('common.query') }}</a
      >{{ t('connector.bottom3end') }}
    </p>
    <p>
      {{ t('connector.bottom3') }}
      <a :href="`${docs.urlPrefix}/programming/connect/rest-api/`">REST API</a>{{ t('connector.bottom3end') }}
    </p>
  </div>
</template>

<script lang="ts" setup>
import DocConfig from '../configTabs.vue';
import { endpoint } from '../utils';
import { t } from 'locales';
import { docs, instance } from 'config';
</script>
