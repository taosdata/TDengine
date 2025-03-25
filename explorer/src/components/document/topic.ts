import * as mdx from "./mdx";
import { t } from "@/lang/index";

export default () => [
  {
    name: "Python",
    icon: "Python",
    docs: {
      zh:mdx.topicPythonDoc,
      en:mdx.topicPythonDoc
    },
    steps: [
      { title: t("docs.topic.python.step1"), dom: "py-install-module" },
      { title: t("docs.topic.step2"), dom: "py-config" },
      { title: t("docs.topic.step3"), dom: "py-create-consumer" },
      { title: t("docs.topic.step4"), dom: "py-subscribe-consume" },
      { title: t("docs.topic.step5"), dom: "py-close-consumer" },
      { title: t("docs.topic.step6"), dom: "py-fullexample" },
    ],
  },
  {
    name: "Go",
    icon: "Go",
    docs: {
      zh:mdx.topicGoDoc,
      en:mdx.topicGoDoc
    },
    steps: [
      { title: t("docs.topic.go.step1"), dom: "go-initialize-module-depend" },
      { title: t("docs.topic.step2"), dom: "go-config" },
      { title: t("docs.topic.step3"), dom: "go-create-consumer" },
      { title: t("docs.topic.step4"), dom: "go-subscribe-consume" },
      { title: t("docs.topic.step5"), dom: "go-close-consumer" },
      { title: t("docs.topic.step6"), dom: "go-fullexample" },
    ],
  },
  {
    name: "Rust",
    icon: "Rust",
    docs: {
      zh:mdx.topicRustDoc,
      en:mdx.topicRustDoc
      
    },
    steps: [
      { title: t("docs.topic.rust.step1"), dom: "rust-create-project" },
      { title: t("docs.topic.step2"), dom: "rust-config" },
      { title: t("docs.topic.step3"), dom: "rust-create-consumer" },
      { title: t("docs.topic.step4"), dom: "rust-subscribe-consume" },
      { title: t("docs.topic.step5"), dom: "rust-close-consumer" },
      { title: t("docs.topic.step6"), dom: "rust-fullexample" },
    ],
  },
  {
    name: 'Java',
    icon: 'Java',
    docs: {
      zh:mdx.topicJavaDoc,
      en:mdx.topicJavaDoc
    },
    steps: [
      { title: t('docs.topic.createProject'), dom: 'init' },
      { title: t('docs.topic.step2'), dom: 'config' },
      { title: t('docs.topic.step3'), dom: 'create-consumer' },
      { title: t('docs.topic.step4'), dom: 'subscribe-consume' },
      { title: t('docs.topic.step5'), dom: 'close-consumer' },
      { title: t('docs.topic.step6'), dom: 'full-example' }
    ]
  }
];
