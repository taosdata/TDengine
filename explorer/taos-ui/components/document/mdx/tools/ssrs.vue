<template>
  <div class="docs">
    <p>
      <a :href="`https://learn.microsoft.com${runtimeURLPart}/sql/reporting-services/`"
        >SQL Server Reporting Services</a
      >
      {{ $t('tools.ssrs.brief') }}
    </p>

    <h2 id="ssrs-prepare">{{ $t('tools.ssrs.step1full') }}</h2>
    <p>{{ $t('tools.ssrs.step1pre1') }}</p>

    <h3>{{ $t('tools.ssrs.step11') }}</h3>
    <ol class="odbc-ol">
      <li class="odbc-span">
        {{ $t('tools.ssrs.step11item1') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.ssrs.step11item2') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.ssrs.step11item3') }}
        <a :href="`https://www.microsoft.com${runtimeURLPart}/sql-server/sql-server-downloads`">{{
          $t('tools.ssrs.step12link1')
        }}</a>
        {{ $t('tools.ssrs.endmark') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.ssrs.step11item4') }}
        <a
          :href="`https://learn.microsoft.com${runtimeURLPart}/sql/reporting-services/install-windows/install-reporting-services?view=sql-server-ver16`"
        >
          {{ $t('tools.ssrs.step11link1') }}
        </a>
        {{ $t('tools.ssrs.endmark') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.ssrs.step11item5') }}
      </li>
    </ol>

    <h3>{{ $t('tools.ssrs.step12') }}</h3>
    <ol class="odbc-ol">
      <li class="odbc-span">
        {{ $t('tools.ssrs.step12item1') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.ssrs.step12item2') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.ssrs.step12item3') }}
        <a :href="`https://www.microsoft.com${runtimeURLPart}/download/details.aspx?id=53613`">
          {{ $t('tools.ssrs.step12link1') }}
        </a>
        {{ $t('tools.ssrs.endmark') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.ssrs.step12item4') }}
      </li>
    </ol>

    <h3>{{ $t('tools.ssrs.step13') }}</h3>
    <ol class="odbc-ol">
      <li class="odbc-span">
        {{ $t('tools.ssrs.step13item1') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.ssrs.step13item2') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.ssrs.step13item3') }}
      </li>
    </ol>

    <h2 id="ssrs-config">{{ $t('tools.ssrs.step2full') }}</h2>
    <p>{{ $t('tools.ssrs.step2pre1') }}</p>
    <ol class="odbc-ol">
      <li class="odbc-span">
        {{ $t('tools.ssrs.step21') }}
        <ul class="odbc-ol">
          <li class="odbc-span">{{ $t('tools.ssrs.step21pre1') }}</li>
          <li class="odbc-span">{{ $t('tools.ssrs.step21item1') }}</li>
          <li class="odbc-span">{{ $t('tools.ssrs.step21item2') }}</li>
          <li class="odbc-span">{{ $t('tools.ssrs.step21item3') }}</li>
          <div class="gf-input">
            <CopyText :text="dsn" class="gf-input-value" />
          </div>
          <li class="odbc-span">{{ $t('tools.ssrs.step21item4') }}</li>
          <li class="odbc-span">{{ $t('tools.ssrs.step21end1') }}</li>
        </ul>
      </li>

      <li class="odbc-span">
        {{ $t('tools.ssrs.step22') }}
        <ul class="odbc-ol">
          <li class="odbc-span">{{ $t('tools.ssrs.step22pre1') }}</li>
          <li class="odbc-span">{{ $t('tools.ssrs.step22end1') }}</li>
        </ul>
      </li>

      <li class="odbc-span">
        {{ $t('tools.ssrs.step23') }}
        <ul class="odbc-ol">
          <li class="odbc-span">{{ $t('tools.ssrs.step23pre1') }}</li>
          <li class="odbc-span">{{ $t('tools.ssrs.step23item1') }}</li>
          <li class="odbc-span">{{ $t('tools.ssrs.step23item2') }}</li>
          <li class="odbc-span">{{ $t('tools.ssrs.step23item3') }}</li>
          <li class="odbc-span">{{ $t('tools.ssrs.step23item4') }}</li>
          <li class="odbc-span">{{ $t('tools.ssrs.step23end1') }}</li>
        </ul>
      </li>
    </ol>

    <h2 id="ssrs-analysis">{{ $t('tools.ssrs.step3full') }}</h2>

    <h3>{{ $t('tools.ssrs.step31') }}</h3>
    <p>{{ $t('tools.ssrs.step31pre1') }}</p>

    <h3>{{ $t('tools.ssrs.step32') }}</h3>
    <p>{{ $t('tools.ssrs.step32pre1') }}</p>

    <h3>{{ $t('tools.ssrs.step33') }}</h3>
    <ol class="odbc-ol">
      <li class="odbc-span">
        {{ $t('tools.ssrs.step331') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.ssrs.step332') }}
        <ul class="odbc-ol">
          <li class="odbc-span">{{ $t('tools.ssrs.step332pre1') }}</li>
          <li class="odbc-span">{{ $t('tools.ssrs.step332item1') }}</li>
          <li class="odbc-span">{{ $t('tools.ssrs.step332item2') }}</li>
          <li class="odbc-span">{{ $t('tools.ssrs.step332item3') }}</li>
          <li class="odbc-span">{{ $t('tools.ssrs.step332item4') }}</li>
        </ul>
      </li>
      <pre
        v-highlight="
          ` SELECT 
        tbname        as DeviceID, 
        last(ts)      as last_ts, 
        last(current) as last_current, 
        last(voltage) as last_voltage 
    FROM test.meters 
    GROUP BY tbname 
    ORDER BY tbname;`
        "
      >
   <code class="language-bash"></code></pre>
      <li class="odbc-span">
        {{ $t('tools.ssrs.step334') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.ssrs.step335') }}
      </li>
    </ol>

    <h3>{{ $t('tools.ssrs.step34') }}</h3>
    <ol class="odbc-ol">
      <li class="odbc-span">
        {{ $t('tools.ssrs.step341') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.ssrs.step342') }}
      </li>
      <li class="odbc-span">
        {{ $t('tools.ssrs.step343') }}
      </li>
    </ol>

    <h3>{{ $t('tools.ssrs.step35') }}</h3>
    <p>{{ $t('tools.ssrs.step35pre1') }}</p>
    <ol class="odbc-ol">
      <li class="odbc-span">
        {{ $t('tools.ssrs.step351') }}
      </li>
      <p>{{ $t('tools.ssrs.step351pre1') }}</p>

      <li class="odbc-span">
        {{ $t('tools.ssrs.step352') }}
      </li>
      <p>{{ $t('tools.ssrs.step352pre1') }}</p>

      <li class="odbc-span">
        {{ $t('tools.ssrs.step353') }}
      </li>
      <p>{{ $t('tools.ssrs.step353pre1') }}</p>
    </ol>

    <h3>{{ $t('tools.ssrs.step36') }}</h3>
    <p>
      {{ $t('tools.ssrs.step36pre1') }}
      <a
        :href="`https://learn.microsoft.com${runtimeURLPart}/sql/reporting-services/report-builder/finding-viewing-and-managing-reports-report-builder-and-ssrs?view=sql-server-ver16`"
      >
        {{ $t('tools.ssrs.step36link1') }}
      </a>
      {{ $t('tools.ssrs.endmark') }}
    </p>
  </div>
</template>

<script lang="ts" setup>
import { isEn } from 'config';
import { dsn } from '../utils';
const runtimeURLPart = !isEn.value ? '/zh-cn' : '/en-us';
</script>
<style lang="scss" scoped>
.docs p {
  line-height: 30px;
}

.odbc-ol {
  padding-left: 0;

  .odbc-span {
    padding-left: 20px;
    line-height: 30px;
    text-indent: -20px;

    .pre-code {
      text-indent: 0;

      // margin-left: -17px;
    }
  }
}

.gf-input {
  display: flex;
  align-items: center;
  width: 70%;
  margin: 10px;
  margin-left: 0;
  font-size: 14px;
  text-indent: 0;

  .gf-input-label {
    width: 135px;
  }

  .gf-input-value {
    flex: 1;
    padding: 0 10px;
    line-height: 40px;
    background-color: #f6f8fa;
    border-radius: 5px;
  }
}
</style>
