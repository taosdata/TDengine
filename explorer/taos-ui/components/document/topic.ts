import * as mdx from './mdx';
import { t } from 'locales';

export default [
  {
    name: 'Python',
    docs: mdx.topicPythonDoc,
    steps: [
      { title: t('topic.python.step1'), dom: 'install-module' },
      { title: t('topic.step2'), dom: 'config' },
      { title: t('topic.step3'), dom: 'create-consumer' },
      { title: t('topic.step4'), dom: 'subscribe-consume' },
      { title: t('topic.step5'), dom: 'close-consumer' },
      { title: t('topic.step6'), dom: 'fullexample' }
    ]
  },
  {
    name: 'Node.js',
    docs: mdx.topicNodeDoc,
    steps: [
      { title: t('topic.node.step1'), dom: 'install-module' },
      { title: t('topic.step2'), dom: 'config' },
      { title: t('topic.step3'), dom: 'create-consumer' },
      { title: t('topic.step4'), dom: 'subscribe-consume' },
      { title: t('topic.step5'), dom: 'close-consumer' },
      { title: t('topic.step6'), dom: 'fullexample' }
    ]
  },
  {
    name: 'Go',
    docs: mdx.topicGoDoc,
    steps: [
      { title: t('topic.go.step1'), dom: 'initialize-module-depend' },
      { title: t('topic.step2'), dom: 'config' },
      { title: t('topic.step3'), dom: 'create-consumer' },
      { title: t('topic.step4'), dom: 'subscribe-consume' },
      { title: t('topic.step5'), dom: 'close-consumer' },
      { title: t('topic.step6'), dom: 'fullexample' }
    ]
  },
  {
    name: 'Rust',
    docs: mdx.topicRustDoc,
    steps: [
      { title: t('topic.createProject'), dom: 'create-project' },
      { title: t('topic.step2'), dom: 'config' },
      { title: t('topic.step3'), dom: 'create-consumer' },
      { title: t('topic.step4'), dom: 'subscribe-consume' },
      { title: t('topic.step5'), dom: 'close-consumer' },
      { title: t('topic.step6'), dom: 'fullexample' }
    ]
  },
  {
    name: 'C#',
    docs: mdx.topicCsharpDoc,
    steps: [
      { title: t('topic.createProject'), dom: 'create-project' },
      { title: t('topic.step2'), dom: 'config' },
      { title: t('topic.step3'), dom: 'create-consumer' },
      { title: t('topic.step4'), dom: 'subscribe-consume' },
      { title: t('topic.step5'), dom: 'close-consumer' },
      { title: t('topic.step6'), dom: 'fullexample' }
    ]
  },
  {
    name: 'Java',
    docs: mdx.topicJavaDoc,
    steps: [
      { title: t('topic.createProject'), dom: 'init' },
      { title: t('topic.step2'), dom: 'config' },
      { title: t('topic.step3'), dom: 'create-consumer' },
      { title: t('topic.step4'), dom: 'subscribe-consume' },
      { title: t('topic.step5'), dom: 'close-consumer' },
      { title: t('topic.step6'), dom: 'full-example' }
    ]
  }
];
