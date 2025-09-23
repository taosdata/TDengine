import * as mdx from './mdx';
import { t } from 'locales';
export default [
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
    name: 'Google Looker Studio',
    desc: t('tools.gds.desc'),
    docs: mdx.gdsDoc,
    steps: [
      { title: t('tools.gds.step1'), dom: 'choose-data-source' },
      { title: t('tools.gds.step2'), dom: 'connector-configuration' },
      { title: t('tools.gds.step3'), dom: 'create-report-or-dashboard' }
    ]
  }
];
