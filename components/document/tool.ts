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
    name: 'Seeq',
    desc: t('tools.seeq.desc'),
    docs: mdx.seeqDoc,
    steps: [
      { title: t('tools.seeq.step1'), dom: 'seeq-repare' },
      { title: t('tools.seeq.step2'), dom: 'seeq-install' },
      { title: t('tools.seeq.step3'), dom: 'seeq-add-ds' },
      { title: t('tools.seeq.step4'), dom: 'seeq-example' }
    ]
  },
  {
    name: 'Google Data Studio',
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
    desc: 'Power BI ' + t('tools.powerbi.desc'),
    docs: mdx.powerbiDoc,
    version: '>=3.2.2.0',
    steps: [
      { title: t('tools.powerbi.step1'), dom: 'powerbi-repare' },
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
      { title: t('tools.yonghongbi.step1'), dom: 'yonghongbi-repare' },
      { title: t('tools.yonghongbi.step2'), dom: 'yonghongbi-install' },
      { title: t('tools.yonghongbi.step3'), dom: 'yonghongbi-config' },
      { title: t('tools.yonghongbi.step4'), dom: 'yonghongbi-createds' },
      { title: t('tools.yonghongbi.step5'), dom: 'yonghongbi-report' }
    ]
  }
];
