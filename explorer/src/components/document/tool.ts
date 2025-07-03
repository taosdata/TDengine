import * as mdx from './mdx';
import { t } from '@/lang/index';
export default () => [
  {
    name: 'TDengine CLI',
    icon: 'tdenginecli',
    desc: t('docs.tool.cli.desc'),
    docs: {
      zh: mdx.TDCLIDoc,
      en: mdx.TDCLIDoc
    },
    steps: [
      { title: t('docs.tool.cli.step1'), dom: 'installation' },
      { title: t('docs.tool.cli.step2'), dom: 'config' },
      { title: t('docs.tool.cli.step3'), dom: 'connect' },
      { title: t('docs.tool.cli.step4'), dom: 'using-tdengine-cli' }
    ]
  },
  {
    name: 'taosBenchmark',
    icon: 'taosBenchmark',
    desc: t('docs.tool.benchmark.desc'),
    docs: {
      zh: mdx.benchmarkDoc,
      en: mdx.benchmarkDoc
    },
    steps: [
      { title: t('docs.tool.benchmark.step1'), dom: 'introduction' },
      { title: t('docs.tool.benchmark.step2'), dom: 'installation' },
      { title: t('docs.tool.benchmark.step3'), dom: 'run' },
      { title: t('docs.tool.benchmark.step4'), dom: 'configuration-file-parameters-in-detailed' }
    ]
  },
  {
    name: 'taosDump',
    icon: 'taosDump',
    desc: t('docs.dataout.dump.desc'),
    docs: {
      zh: mdx.taosDumpDoc,
      en: mdx.taosDumpDoc
    },
    steps: [
      { title: t('docs.dataout.dump.step1'), dom: 'introduction' },
      { title: t('docs.dataout.dump.step2'), dom: 'installation' },
      { title: t('docs.dataout.dump.step3'), dom: 'common-usage-scenarios' },
      { title: t('docs.dataout.dump.step4'), dom: 'detailed-command-line-parameter-list' }
    ]
  },
  {
    name: 'Grafana',
    icon: 'Grafana',
    desc: t('docs.virtual.grafana.desc'),
    docs: {
      zh: mdx.grafanaDoc,
      en: mdx.grafanaDoc
    },
    steps: [
      { title: t('docs.virtual.grafana.step1'), dom: 'install-grafana' },
      { title: t('docs.virtual.grafana.step2'), dom: 'install-tdengine-plugin' },
      { title: t('docs.virtual.grafana.step3'), dom: 'verify-plugin' },
      { title: t('docs.virtual.grafana.step4'), dom: 'use-grafana' }
    ]
  },
  {
    name: 'Seeq',
    icon: 'Seeq',
    desc: t('docs.tools.seeq.desc'),
    docs: {
      zh: mdx.seeqDoc,
      en: mdx.seeqDoc
    },
    steps: [
      { title: t('docs.tools.seeq.step1'), dom: 'seeq-repare' },
      { title: t('docs.tools.seeq.step2'), dom: 'seeq-install' },
      { title: t('docs.tools.seeq.step3'), dom: 'seeq-add-ds' },
      { title: t('docs.tools.seeq.step4'), dom: 'seeq-example' }
    ]
  },
  {
    name: 'Looker Studio',
    icon: 'gdStudio',
    desc: t('docs.virtual.gds.desc'),
    docs: {
      zh: mdx.gdsDoc,
      en: mdx.gdsDoc
    },
    steps: [
      { title: t('docs.virtual.gds.step1'), dom: 'choose-data-source' },
      { title: t('docs.virtual.gds.step2'), dom: 'connector-configuration' },
      { title: t('docs.virtual.gds.step3'), dom: 'create-report-or-dashboard' }
    ]
  },
  {
    name: 'PowerBI',
    icon: 'powerbilogo',
    desc: 'Power BI' + t('docs.tools.powerbi.desc'),
    docs: {
      zh: mdx.powerbiDoc,
      en: mdx.powerbiDoc
    },
    version: '>=3.2.2.0',
    steps: [
      { title: t('docs.tools.powerbi.step1'), dom: 'powerbi-repare' },
      { title: t('docs.tools.powerbi.step2'), dom: 'powerbi-install' },
      { title: t('docs.tools.powerbi.step3'), dom: 'powerbi-config' },
      { title: t('docs.tools.powerbi.step4'), dom: 'powerbi-import' },
      { title: t('docs.tools.powerbi.step5'), dom: 'powerbi-example' }
    ]
  },
  {
    name: 'YonghongBI',
    title: t('docs.tools.yonghongbi.name'),
    icon: 'yonghonglogo',
    desc: t('docs.tools.yonghongbi.desc') + t('docs.tools.yonghongbi.desc1'),
    docs: {
      zh: mdx.yonghongbiDoc,
      en: mdx.yonghongbiDoc
    },
    version: '>=3.2.2.0',
    isAli: true,
    steps: [
      { title: t('docs.tools.yonghongbi.step1'), dom: 'yonghongbi-repare' },
      { title: t('docs.tools.yonghongbi.step2'), dom: 'yonghongbi-install' },
      { title: t('docs.tools.yonghongbi.step3'), dom: 'yonghongbi-config' },
      { title: t('docs.tools.yonghongbi.step4'), dom: 'yonghongbi-createds' },
      { title: t('docs.tools.yonghongbi.step5'), dom: 'yonghongbi-report' }
    ]
  },
  {
    name: 'Superset',
    title: t('docs.tools.superset.name'),
    icon: 'superset-logo',
    desc: t('docs.tools.superset.desc'),
    docs: {
      zh: mdx.supersetDoc,
      en: mdx.supersetDoc
    },
    version: '>=3.3.5.0',
    isAli: true,
    steps: [
      { title: t('docs.tools.superset.step1'), dom: 'superset-repare' },
      { title: t('docs.tools.superset.step2'), dom: 'superset-install' },
      { title: t('docs.tools.superset.step3'), dom: 'superset-config' },
      { title: t('docs.tools.superset.step4'), dom: 'superset-import' },
      { title: t('docs.tools.superset.step5'), dom: 'superset-example' }
    ]
  },
  {
    name: 'Excel',
    title: t('docs.tools.excel.name'),
    icon: 'excel-logo',
    desc: t('docs.tools.excel.desc'),
    docs: {
      zh: mdx.excelDoc,
      en: mdx.excelDoc
    },
    version: '>=3.3.5.0',
    isAli: true,
    steps: [
      { title: t('docs.tools.excel.step1'), dom: 'excel-repare' },
      { title: t('docs.tools.excel.step2'), dom: 'excel-install' },
      { title: t('docs.tools.excel.step3'), dom: 'excel-config' },
      { title: t('docs.tools.excel.step4'), dom: 'excel-import' },
      { title: t('docs.tools.excel.step5'), dom: 'excel-example' }
    ]
  },
  {
    name: 'Tableau',
    title: t('docs.tools.tableau.name'),
    icon: 'tableau-logo',
    desc: t('docs.tools.tableau.desc'),
    docs: {
      zh: mdx.tableauDoc,
      en: mdx.tableauDoc
    },
    version: '>=3.3.5.0',
    isAli: true,
    steps: [
      { title: t('docs.tools.tableau.step1'), dom: 'tableau-repare' },
      { title: t('docs.tools.tableau.step2'), dom: 'tableau-install' },
      { title: t('docs.tools.tableau.step3'), dom: 'tableau-config' },
      { title: t('docs.tools.tableau.step4'), dom: 'tableau-import' },
      { title: t('docs.tools.tableau.step5'), dom: 'tableau-example' }
    ]
  },
  {
    name: 'Node-RED',
    desc: t('docs.tools.nodered.desc'),
    docs: {
      zh: mdx.noderedDoc,
      en: mdx.noderedDoc
    },
    version: '>=3.3.2.0',
    steps: [
      { title: t('docs.tools.nodered.step1'), dom: 'nodered-prepare' },
      { title: t('docs.tools.nodered.step2'), dom: 'nodered-config'  },
      { title: t('docs.tools.nodered.step3'), dom: 'nodered-analysis'},
      { title: t('docs.tools.nodered.step4'), dom: 'nodered-summary'}
    ]
  }  
];
