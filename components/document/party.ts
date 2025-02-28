import * as mdx from './mdx';
import { t } from 'locales';
import { project } from 'config';

const cloudText = project.isCloud ? 'Cloud' : '';

export default [
  {
    name: 'Prometheus',
    desc: t('dataIn.prometheus.desc', [cloudText]),
    docs: mdx.prometheusDoc,
    steps: [
      { title: t('dataIn.prometheus.step1'), dom: 'prerequisites' },
      { title: t('dataIn.prometheus.step2'), dom: 'install-prometheus' },
      { title: t('dataIn.prometheus.step3'), dom: 'configure-prometheus' },
      { title: t('dataIn.prometheus.step4'), dom: 'start-prometheus' },
      { title: t('dataIn.prometheus.step5'), dom: 'verify-remote-write' }
    ]
  },
  {
    name: 'Telegraf',
    desc: t('dataIn.telegraf.desc', [cloudText]),
    docs: mdx.telegrafDoc,
    steps: [
      { title: t('dataIn.telegraf.step1'), dom: 'prerequisites' },
      { title: t('dataIn.telegraf.step2'), dom: 'install-telegraf' },
      { title: t('dataIn.telegraf.step3'), dom: 'configure' },
      { title: t('dataIn.telegraf.step4'), dom: 'start-telegraf' },
      { title: t('dataIn.telegraf.step5'), dom: 'verify' }
    ]
  },
  {
    name: t('dataIn.influxdb.title'),
    icon: 'influxDB',
    desc: t('dataIn.influxdb.desc', [t('dataIn.influxdb.title'), cloudText]),
    docs: mdx.influxDBDoc,
    steps: [
      { title: t('dataIn.influxdb.step1'), dom: 'config' },
      { title: t('dataIn.influxdb.step2'), dom: 'insert' },
      { title: t('dataIn.influxdb.step3'), dom: 'examples' }
    ]
  },
  {
    name: t('dataIn.opentsdbjson.title'),
    icon: 'openTSDB',
    desc: t('dataIn.influxdb.desc', [t('dataIn.opentsdbjson.title'), cloudText]),
    docs: mdx.opentsJSONDoc,
    steps: [
      { title: t('dataIn.influxdb.step1'), dom: 'config' },
      { title: t('dataIn.influxdb.step2'), dom: 'insert' },
      { title: t('dataIn.influxdb.step3'), dom: 'examples' }
    ]
  },
  {
    name: t('dataIn.opentsdbtelnet.title'),
    icon: 'openTSDB',
    desc: t('dataIn.influxdb.desc', [t('dataIn.opentsdbtelnet.title'), cloudText]),
    docs: mdx.opentsTelnetDoc,
    steps: [
      { title: t('dataIn.influxdb.step1'), dom: 'config' },
      { title: t('dataIn.influxdb.step2'), dom: 'insert' },
      { title: t('dataIn.influxdb.step3'), dom: 'examples' }
    ]
  }
];
