import * as mdx from './mdx';
import { t } from 'locales';

export default [
  {
    name: 'Java',
    type: 'client',
    desc: t('connector.desc', ['taos-jdbc']),
    docs: mdx.javaDoc,
    steps: [
      { title: t('connector.java.step1'), dom: 'add-dependency' },
      { title: t('connector.java.step2'), dom: 'config' },
      { title: t('connector.java.step3'), dom: 'connect' }
    ]
  },
  {
    name: 'Go',
    type: 'client',
    desc: t('connector.desc', ['driver-go']),
    docs: mdx.goDoc,
    steps: [
      { title: t('connector.go.step1'), dom: 'initialize-module' },
      { title: t('connector.go.step2'), dom: 'add-dependency' },
      { title: t('connector.go.step3'), dom: 'config' },
      { title: t('connector.go.step4'), dom: 'connect' }
    ]
  },
  {
    name: 'Python',
    type: 'client',
    desc: t('connector.desc', ['taospy']),
    docs: mdx.pythonDoc,
    steps: [
      { title: t('connector.python.step1'), dom: 'install-connector' },
      { title: t('connector.python.step2'), dom: 'config' },
      { title: t('connector.python.step3'), dom: 'connect' },
      { title: 'Jupyter', dom: 'jupyter' }
    ]
  },
  {
    name: 'Node.js',
    type: 'client',
    desc: t('connector.desc', ['@tdengine/rest']),
    docs: mdx.nodeDoc,
    steps: [
      { title: t('connector.node.step1'), dom: 'install-connector' },
      { title: t('connector.node.step2'), dom: 'config' },
      { title: t('connector.node.step3'), dom: 'connect' }
    ]
  },
  {
    name: 'C#',
    type: 'client',
    icon: 'csharp',
    desc: t('connector.desc', ['TDengine.Connector']),
    docs: mdx.csharpDoc,
    steps: [
      { title: t('connector.csharp.step1'), dom: 'create-project' },
      { title: t('connector.csharp.step2'), dom: 'config' },
      { title: t('connector.csharp.step3'), dom: 'connect' }
    ]
  },
  {
    name: 'Rust',
    type: 'client',
    desc: t('connector.desc', ['taos']),
    docs: mdx.rustDoc,
    steps: [
      { title: t('connector.rust.step1'), dom: 'create-project' },
      { title: t('connector.rust.step2'), dom: 'add-dependency' },
      { title: t('connector.rust.step3'), dom: 'config' },
      { title: t('connector.rust.step4'), dom: 'connect' }
    ]
  },
  {
    name: 'R',
    type: 'client',
    desc: t('connector.desc', ['taos']),
    docs: mdx.rDoc,
    steps: [
      { title: t('connector.r.step1'), dom: 'create-project' },
      { title: t('connector.r.step2'), dom: 'config' },
      { title: t('connector.r.step3'), dom: 'connect' }
    ]
  },
  {
    name: 'ODBC',
    type: 'client',
    desc: t('connector.odbc.desc') + 'Power BI' + t('connector.odbc.desc1'),
    docs: mdx.odbcDoc,
    version: '>=3.2.1.0',
    steps: [
      { title: t('connector.odbc.step1'), dom: 'install' },
      { title: t('connector.odbc.step2'), dom: 'config' },
      { title: t('connector.odbc.step3'), dom: 'example' }
    ]
  },
  {
    name: 'REST API',
    type: 'client',
    desc: t('connector.rest.desc'),
    docs: mdx.restDoc,
    steps: [
      { title: t('connector.rest.step1'), dom: 'config' },
      { title: t('connector.rest.step2'), dom: 'insert' },
      { title: t('connector.rest.step3'), dom: 'query' }
    ]
  }
];
