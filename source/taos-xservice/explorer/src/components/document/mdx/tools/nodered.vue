<template>
  <div class="docs">
    <p><a :href="`https://nodered.org/`">Node-RED</a> {{ $t('docs.tools.nodered.brief1') }} </p>
    <p>{{ $t('docs.tools.nodered.brief2') }}</p>
    <p>{{ $t('docs.tools.nodered.brief3') }}</p>
    <ol class="odbc-ol">
      <li class="odbc-span">
        {{ $t('docs.tools.nodered.briefitem1') }}
      </li>
      <li class="odbc-span">
        {{ $t('docs.tools.nodered.briefitem2') }}
      </li>
    </ol> 

    <h2 id="nodered-prepare">{{ $t('docs.tools.nodered.step1') }}</h2>
    <p> {{ $t('docs.tools.nodered.step1pre1') }} </p>

    <ol class="odbc-ol">
      <li class="odbc-span">
        {{ $t('docs.tools.nodered.step1item1') }}
        <a :href="`https://nodered.org/docs/getting-started/`">{{ $t('docs.tools.nodered.step11link1') }}</a>
        {{ $t('docs.tools.nodered.endmark') }}
      </li>
      <li class="odbc-span">
        {{ $t('docs.tools.nodered.step1item2') }}
        <a :href="`https://www.npmjs.com/package/@tdengine/websocket`">{{ $t('docs.tools.nodered.step12link1') }}</a>
        {{ $t('docs.tools.nodered.endmark') }}        
      </li>
      <li class="odbc-span">
        {{ $t('docs.tools.nodered.step1item3') }}
        <a :href="`https://www.npmjs.com/package/node-red-node-tdengine`">{{ $t('docs.tools.nodered.step13link1') }}</a>
        {{ $t('docs.tools.nodered.endmark') }}        
      </li>      
    </ol>

    <h2 id = "nodered-config">{{ $t('docs.tools.nodered.step2') }}</h2>
    <p> {{ $t('docs.tools.nodered.step2pre1') }} </p>
    <ol class="odbc-ol">
      <li class="odbc-span">
        {{ $t('docs.tools.nodered.step21') }}
      </li>

      <li class="odbc-span">
        {{ $t('docs.tools.nodered.step22') }}
      </li>

      <li class="odbc-span">
        {{ $t('docs.tools.nodered.step23') }}
          <div class="gf-input">
            <CopyText
              :text="DSN"
              class="gf-input-value"
            />
          </div>
      </li>

      <li class="odbc-span">
        {{ $t('docs.tools.nodered.step24') }}
      </li>
    </ol>
 
    <h2 id = "nodered-analysis">{{ $t('docs.tools.nodered.step3') }}</h2>
    
      <h3>{{ $t('docs.tools.nodered.step31') }}</h3>

      <h4>{{ $t('docs.tools.nodered.step311') }}</h4>
      <p> {{ $t('docs.tools.nodered.step311pre1') }} </p>
      <p> {{ $t('docs.tools.nodered.step311pre2') }} </p>
      <ol class="odbc-ol">
        <li class="odbc-span">
          {{ $t('docs.tools.nodered.step311item1') }}
        </li>
        <li class="odbc-span">
          {{ $t('docs.tools.nodered.step311item2') }}
        </li>
        <li class="odbc-span">
          {{ $t('docs.tools.nodered.step311item3') }}
        </li>
        <li class="odbc-span">
          {{ $t('docs.tools.nodered.step311item4') }}
        </li>
      </ol>

      <p> {{ $t('docs.tools.nodered.step311sec1') }} </p>
      <ol class="odbc-ol">
        <li class="odbc-span">
          {{ $t('docs.tools.nodered.step311secitem1') }}
        </li>
        <li class="odbc-span">
          {{ $t('docs.tools.nodered.step311secitem2') }}
        </li>
      </ol>

      <h4>{{ $t('docs.tools.nodered.step312') }}</h4>
      <p> {{ $t('docs.tools.nodered.step312pre1') }} </p>
      <ol class="odbc-ol">
        <li class="odbc-span">
          {{ $t('docs.tools.nodered.step312item1') }}
        </li>
        <li class="odbc-span">
          {{ $t('docs.tools.nodered.step312item2') }}
        </li>
      </ol>
      <p> {{ $t('docs.tools.nodered.step312pre2') }} </p>

<pre v-highlight><code>create database test;
create stable test.meters (ts timestamp, current float, voltage int, phase float ) 
                     tags (groupid int, location varchar(24));
create table test.d0 using test.meters tags(1, 'workshop1');
create table test.d1 using test.meters tags(2, 'workshop1');
create table test.d2 using test.meters tags(2, 'workshop2');      
</code></pre>


      <h3>{{ $t('docs.tools.nodered.step32') }}</h3>

      <h4>{{ $t('docs.tools.nodered.step321') }}</h4>
      <p> {{ $t('docs.tools.nodered.step321pre1') }} </p>
      <p> {{ $t('docs.tools.nodered.step321pre2') }} </p>

      <p><b> {{ $t('docs.tools.nodered.step3211') }} </b></p>
      <ol class="odbc-ol">
        <li class="odbc-span">
          {{ $t('docs.tools.nodered.step3211item1') }}
        </li>
        <li class="odbc-span">
          {{ $t('docs.tools.nodered.step3211item2') }}
        </li>
        <li class="odbc-span">
          {{ $t('docs.tools.nodered.step3211item3') }}
          <div class="gf-input">
            <CopyText
              :text="DSN"
              class="gf-input-value"
            />
          </div>
        </li>
        <li class="odbc-span">
          {{ $t('docs.tools.nodered.step3211item4') }}
        </li>
      </ol>
      <p><b> {{ $t('docs.tools.nodered.step3212') }} </b></p>
      <ol class="odbc-ol">
        <li class="odbc-span">
          {{ $t('docs.tools.nodered.step3212item1') }}
        </li>
        <li class="odbc-span">
          {{ $t('docs.tools.nodered.step3212item2') }}

<pre v-highlight><code>// generate rand
const value2 = Math.floor(Math.random() * (30 - 5 + 1)) + 5; // 5-30
const value3 = Math.floor(Math.random() * (240 - 198 + 1)) + 198; // 198-240
const value4 = Math.floor(Math.random() * (3 - 1 + 1)) + 1; // 1-3

// sql
msg.topic = `insert into test.d0 values (now, ${value2}, ${value3}, ${value4});`;

return msg;
</code></pre>

        </li>
        <li class="odbc-span">
          {{ $t('docs.tools.nodered.step3212item3') }}
        </li>
        <li class="odbc-span">
          {{ $t('docs.tools.nodered.step3212item4') }}
        </li>
        <li class="odbc-span">
          {{ $t('docs.tools.nodered.step3212item5') }}
        </li>
      </ol>

      <p><b> {{ $t('docs.tools.nodered.step3213') }} </b></p>
      <ol class="odbc-ol">
        <li class="odbc-span">
          {{ $t('docs.tools.nodered.step3213item1') }}
        </li>
        <li class="odbc-span">
          {{ $t('docs.tools.nodered.step3213item2') }}
        </li>
      </ol>  

      <p> {{ $t('docs.tools.nodered.step321secpre1') }} </p>
      <p> {{ $t('docs.tools.nodered.step321secpre2') }} </p>
      <ol class="odbc-ol">
        <li class="odbc-span">
          {{ $t('docs.tools.nodered.step321secitem1') }}
        </li>
        <li class="odbc-span">
          {{ $t('docs.tools.nodered.step321secitem2') }}
        </li>
      </ol>
      <p> {{ $t('docs.tools.nodered.step321secpre3') }} </p>

<pre v-highlight><code>{
  "topic":  "insert into test.d1 values (now, 20, 203, 2);",
  "isQuery":  false,
  "payload":{
    "affectRows": 1,
    "totalTime":  2,
    "timing":     "961982"
  }
}     
</code></pre>


      <h3>{{ $t('docs.tools.nodered.step322') }}</h3>

      <p> {{ $t('docs.tools.nodered.step322pre1') }} </p>
      <p> {{ $t('docs.tools.nodered.step322pre2') }} </p>
      <ol class="odbc-ol">
        <li class="odbc-span">
          {{ $t('docs.tools.nodered.step322item1') }}

<pre v-highlight><code>select tbname, avg(current), avg(voltage), sum(p) 
from ( select tbname,current,voltage,current*voltage/60 as p from test.meters 
       where  ts > now-60s partition by tbname)
group by tbname;
</code></pre>

        </li>
        <li class="odbc-span">
          {{ $t('docs.tools.nodered.step322item2') }}
        </li>
        <li class="odbc-span">
          {{ $t('docs.tools.nodered.step322item3') }}
        </li>
        <li class="odbc-span">
          {{ $t('docs.tools.nodered.step322item4') }}
        </li>
      </ol>
      <p> {{ $t('docs.tools.nodered.step322pre3') }} </p>
      <ol class="odbc-ol">
        <li class="odbc-span">
          {{ $t('docs.tools.nodered.step322secitem1') }}
        </li>
        <li class="odbc-span">
          {{ $t('docs.tools.nodered.step322secitem2') }}
        </li>
      </ol>
      <p> {{ $t('docs.tools.nodered.step322pre4') }} </p>

<pre v-highlight><code>{
  "topic":  "select tbname,avg(current) ...",
  "isQuery":  true,
  "payload": [
    {
      "tbname":      "d2",
      "avg(current)": 26.7,
      "avg(voltage)": 235,
      "sum(p)":       6329
    },
    {
      "tbname":       "d0",
      "avg(current)": 16.5,
      "avg(voltage)": 222,
      "sum(p)":       121
    },
    {
      "tbname":       "d1",
      "avg(current)": 29,
      "avg(voltage)": 202,
      "sum(p)":       5833
    }
  ]
}
</code></pre>

      <h3>{{ $t('docs.tools.nodered.step323') }}</h3>

      <p> {{ $t('docs.tools.nodered.step323pre1') }} </p>
      <p> {{ $t('docs.tools.nodered.step323pre2') }} </p>
      <ol class="odbc-ol">
        <li class="odbc-span">
          {{ $t('docs.tools.nodered.step323item1') }}

<pre v-highlight><code>create topic topic_overload as
  select tbname,* from test.meters 
  where current > 25 or voltage > 230;
</code></pre>

        </li>
        <li class="odbc-span">
          {{ $t('docs.tools.nodered.step323item2') }}
          <ol class="odbc-ol">
            <li class="odbc-span">
              {{ $t('docs.tools.nodered.step323item2opt1') }}
            </li>
            <li class="odbc-span">
              {{ $t('docs.tools.nodered.step323item2opt2') }}
              <div class="gf-input">
                <CopyText
                  :text="DSN"
                  class="gf-input-value"
                />
              </div>
            </li>
            <li class="odbc-span">
              {{ $t('docs.tools.nodered.step323item2opt3') }}
            </li>
            <li class="odbc-span">
              {{ $t('docs.tools.nodered.step323item2opt4') }}
            </li>
            <li class="odbc-span">
              {{ $t('docs.tools.nodered.step323item2opt5') }}
            </li>
            <li class="odbc-span">
              {{ $t('docs.tools.nodered.step323item2opt6') }}
            </li>
            <li class="odbc-span">
              {{ $t('docs.tools.nodered.step323item2opt7') }}
            </li>
          </ol>
        </li>
        <li class="odbc-span">
          {{ $t('docs.tools.nodered.step323item3') }}
        </li>
        <li class="odbc-span">
          {{ $t('docs.tools.nodered.step323item4') }}
        </li>
      </ol>
      <p> {{ $t('docs.tools.nodered.step323pre3') }} </p>
      <p> {{ $t('docs.tools.nodered.step323pre4') }} </p>

<pre v-highlight><code>{
  "topic": "topic_overload",
  "payload": [
    {
      "tbname":   "d1",
      "ts":       "1750140456777",
      "current":  31,
      "voltage":  217,
      "phase":    2,
      "groupid":  4,
      "location": "California.MountainView"
    }
  ],
  "database":  "test",
  "vgroup_id": 4,
  "precision": 0
}
</code></pre>

      <h3>{{ $t('docs.tools.nodered.step33') }}</h3>

      <p> {{ $t('docs.tools.nodered.step33pre1') }} </p>
      <ol class="odbc-ol">
        <li class="odbc-span">
          {{ $t('docs.tools.nodered.step33item1') }}
        </li>
        <li class="odbc-span">
          {{ $t('docs.tools.nodered.step33item2') }}
        </li>
        <li class="odbc-span">
          {{ $t('docs.tools.nodered.step33item3') }}
        </li>
        <li class="odbc-span">
          {{ $t('docs.tools.nodered.step33item4') }}
        </li>
        <li class="odbc-span">
          {{ $t('docs.tools.nodered.step33item5') }}
        </li>        
      </ol>
      <p> {{ $t('docs.tools.nodered.step33pre2') }} </p>
      <ol class="odbc-ol">
        <li class="odbc-span">
          {{ $t('docs.tools.nodered.step33secitem1') }}
        </li>
        <li class="odbc-span">
          {{ $t('docs.tools.nodered.step33secitem2') }}
        </li>
      </ol>

      <h2 id = "nodered-summary">{{ $t('docs.tools.nodered.step4') }}</h2>

      <p> {{ $t('docs.tools.nodered.step4pre1') }} </p>
      <ol class="odbc-ol">
        <li class="odbc-span">
          {{ $t('docs.tools.nodered.step4item1') }}
          <ol class="odbc-ol">
            <li class="odbc-span">
              {{ $t('docs.tools.nodered.step4item1opt1') }}
            </li>
            <li class="odbc-span">
              {{ $t('docs.tools.nodered.step4item1opt2') }}
            </li>
            <li class="odbc-span">
              {{ $t('docs.tools.nodered.step4item1opt3') }}
            </li>
          </ol>
        </li>
        <li class="odbc-span">
          {{ $t('docs.tools.nodered.step4item2') }}
        </li>
        <li class="odbc-span">
          {{ $t('docs.tools.nodered.step4item3') }}
        </li>
      </ol>
      <p> {{ $t('docs.tools.nodered.docend') }} </p>

  </div>
</template>

<script setup lang="ts">
import { DocsProps } from '../utils';

const props = defineProps<DocsProps>();

const DSN = computed(() => {
  return `ws://${props.user}:${props.password}@${props.url.replace(/^[a-z]+:\/\//, '')}`;
});
</script>

<style lang="scss" scoped>
.docs p {
  line-height: 30px;
}
.odbc-ol {
  padding-left: 0;
  .odbc-span {
    line-height: 30px;
    padding-left: 20px;
    text-indent: -20px;
    .pre-code {
      text-indent: 0px;
      // margin-left: -17px;
    }
  }
}
.gf-input {
  width: 70%;
  display: flex;
  align-items: center;
  margin: 10px;
  margin-left: 0px;
  font-size: 14px;
  text-indent: 0px;
  .gf-input-label {
    width: 135px;
  }
  .gf-input-value {
    flex: 1;
    background-color: #f6f8fa;
    border-radius: 5px;
    line-height: 40px;
    padding: 0 10px;
  }
}
</style>
