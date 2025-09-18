import * as mdx from './mdx';
import { t } from 'locales';

export default [
  {
    name: 'TDengine CLI',
    desc: t('tools.cli.desc'),
    docs: mdx.TDCLIDoc,
    steps: [
      { title: t('tools.cli.step1'), dom: 'installation' },
      { title: t('tools.cli.step2'), dom: 'config' },
      { title: t('tools.cli.step3'), dom: 'connect' },
      { title: t('tools.cli.step4'), dom: 'using-tdengine-cli' }
    ]
  },
  {
    name: 'taosBenchmark',
    desc: t('tools.benchmark.desc'),
    docs: mdx.benchmarkDoc,
    steps: [
      { title: t('tools.benchmark.step1'), dom: 'introduction' },
      { title: t('tools.benchmark.step2'), dom: 'installation' },
      { title: t('tools.benchmark.step3'), dom: 'run' },
      { title: t('tools.benchmark.step4'), dom: 'configuration-file-parameters-in-detailed' }
    ]
  },
  {
    name: 'taosDump',
    desc: t('tools.dump.desc'),
    docs: mdx.dumpDoc,
    steps: [
      { title: t('tools.dump.step1'), dom: 'introduction' },
      { title: t('tools.dump.step2'), dom: 'installation' },
      { title: t('tools.dump.step3'), dom: 'common-usage-scenarios' },
      { title: t('tools.dump.step4'), dom: 'detailed-command-line-parameter-list' }
    ]
  },
  {
    name: 'Grafana',
    desc: t('tools.grafana.desc'),
    docs: mdx.grafanaDoc,
    steps: [
      { title: t('tools.grafana.step1'), dom: 'install-grafana' },
      { title: t('tools.grafana.step2'), dom: 'install-tdengine-plugin' },
      { title: t('tools.grafana.step3'), dom: 'verify-plugin' },
      { title: t('tools.grafana.step4'), dom: 'use-grafana' }
    ]
  },
  {
    name: 'Perspective',
    desc: 'Perspective' + t('tools.perspective.desc'),
    docs: mdx.perspectiveDoc,
    steps: [
      { title: t('tools.perspective.step1'), dom: 'perspective-introduction' },
      { title: t('tools.perspective.step2'), dom: 'perspective-prepare' },
      { title: t('tools.perspective.step3'), dom: 'perspective-import' },
      { title: t('tools.perspective.step4'), dom: 'perspective-viewer' }
    ]
  },
  {
    name: 'Seeq',
    desc: t('tools.seeq.desc'),
    docs: mdx.seeqDoc,
    steps: [
      { title: t('tools.seeq.step1'), dom: 'seeq-prepare' },
      { title: t('tools.seeq.step2'), dom: 'seeq-install' },
      { title: t('tools.seeq.step3'), dom: 'seeq-add-ds' },
      { title: t('tools.seeq.step4'), dom: 'seeq-example' }
    ]
  },
  {
    name: 'Google Looker Studio',
    desc: t('tools.gds.desc'),
    docs: mdx.gdsDoc,
    steps: [
      { title: t('tools.gds.step1'), dom: 'choose-data-source' },
      { title: t('tools.gds.step2'), dom: 'connector-configuration' },
      { title: t('tools.gds.step3'), dom: 'create-report-or-dashboard' }
    ]
  },
  {
    name: 'Power BI',
    desc: 'Power BI' + t('tools.powerbi.desc'),
    docs: mdx.powerbiDoc,
    version: '>=3.2.2.0',
    steps: [
      { title: t('tools.powerbi.step1'), dom: 'powerbi-prepare' },
      { title: t('tools.powerbi.step2'), dom: 'powerbi-install' },
      { title: t('tools.powerbi.step3'), dom: 'powerbi-config' },
      { title: t('tools.powerbi.step4'), dom: 'powerbi-import' },
      { title: t('tools.powerbi.step5'), dom: 'powerbi-example' }
    ]
  },
  {
    name: '永洪 BI',
    desc: t('tools.yonghongbi.desc') + t('tools.yonghongbi.desc1'),
    docs: mdx.yonghongbiDoc,
    version: '>=3.2.1.0',
    isAli: true,
    steps: [
      { title: t('tools.yonghongbi.step1'), dom: 'yonghongbi-prepare' },
      { title: t('tools.yonghongbi.step2'), dom: 'yonghongbi-install' },
      { title: t('tools.yonghongbi.step3'), dom: 'yonghongbi-config' },
      { title: t('tools.yonghongbi.step4'), dom: 'yonghongbi-createds' },
      { title: t('tools.yonghongbi.step5'), dom: 'yonghongbi-report' }
    ]
  },
  {
    name: 'Superset',
    desc: 'Superset' + t('tools.superset.desc'),
    docs: mdx.supersetDoc,
    version: '>=3.2.3.0',
    steps: [
      { title: t('tools.superset.step1'), dom: 'superset-prepare' },
      { title: t('tools.superset.step2'), dom: 'superset-config' },
      { title: t('tools.superset.step3'), dom: 'superset-import' },
      { title: t('tools.superset.step4'), dom: 'superset-analysis' }
    ]
  },
  {
    name: 'Tableau',
    desc: 'Tableau' + t('tools.tableau.desc'),
    docs: mdx.tableauDoc,
    version: '>=3.3.5.8',
    steps: [
      { title: t('tools.tableau.step1'), dom: 'tableau-prepare' },
      { title: t('tools.tableau.step2'), dom: 'tableau-install' },
      { title: t('tools.tableau.step3'), dom: 'tableau-config' },
      { title: t('tools.tableau.step4'), dom: 'tableau-import' },
      { title: t('tools.tableau.step5'), dom: 'tableau-example' }
    ]
  },
  {
    name: 'Excel',
    desc: 'Excel' + t('tools.excel.desc'),
    docs: mdx.excelDoc,
    version: '>=3.3.5.8',
    steps: [
      { title: t('tools.excel.step1'), dom: 'excel-prepare' },
      { title: t('tools.excel.step2'), dom: 'excel-install' },
      { title: t('tools.excel.step3'), dom: 'excel-config' },
      { title: t('tools.excel.step4'), dom: 'excel-import' },
      { title: t('tools.excel.step5'), dom: 'excel-example' }
    ]
  },
  {
    name: 'FineBI',
    desc: t('tools.finebi.desc'),
    docs: mdx.finebiDoc,
    version: '>=3.3.4.0',
    steps: [
      { title: t('tools.finebi.step1'), dom: 'finebi-prepare' },
      { title: t('tools.finebi.step2'), dom: 'finebi-install' },
      { title: t('tools.finebi.step3'), dom: 'finebi-config' },
      { title: t('tools.finebi.step4'), dom: 'finebi-import' },
      { title: t('tools.finebi.step5'), dom: 'finebi-example' }
    ]
  },
  {
    name: 'SSRS',
    desc: t('tools.ssrs.desc'),
    docs: mdx.ssrsDoc,
    version: '>=3.3.3.0',
    steps: [
      { title: t('tools.ssrs.step1'), dom: 'ssrs-prepare' },
      { title: t('tools.ssrs.step2'), dom: 'ssrs-config' },
      { title: t('tools.ssrs.step3'), dom: 'ssrs-analysis' }
    ]
  },
  {
    name: 'Node-RED',
    desc: t('tools.nodered.desc'),
    docs: mdx.noderedDoc,
    version: '>=3.3.2.0',
    steps: [
      { title: t('tools.nodered.step1'), dom: 'nodered-prepare' },
      { title: t('tools.nodered.step2'), dom: 'nodered-config' },
      { title: t('tools.nodered.step3'), dom: 'nodered-analysis' },
      { title: t('tools.nodered.step4'), dom: 'nodered-summary' }
    ]
  }
];
