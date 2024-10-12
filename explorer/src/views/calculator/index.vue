<template>
  <div class="page-wrapper calculator">
    <MainContentHeader :title="$t('calculator.title')"> </MainContentHeader>
    <section class="content">
      <p class="primary-tip">
        {{ $t("calculator.tip") }}
      </p>
      <panelHeader :title="$t('dataIn.inputCSVFile')" />
      <InputCsvFile />
      <Assumptions />
    </section>
  </div>
</template>

<script>
  import panelHeader from "@/components/panelHeader.vue";
  import InputCsvFile from "./components/inputCSVFile.vue";
  import Assumptions from "./components/assumptions.vue";
  export default {
    components: { panelHeader, InputCsvFile, Assumptions },
  };
</script>

<style></style>
