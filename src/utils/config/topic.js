import * as mdx from "./mdx";
import i18n from "@/lang";

export default [
  {
    name: "Python",
    docs: mdx.topicPythonDoc,
    steps: [
      { title: i18n.t("docs.topic.python.step1"), dom: "py-install-module" },
      { title: i18n.t("docs.topic.step2"), dom: "py-config" },
      { title: i18n.t("docs.topic.step3"), dom: "py-create-consumer" },
      { title: i18n.t("docs.topic.step4"), dom: "py-subscribe-consume" },
      { title: i18n.t("docs.topic.step5"), dom: "py-close-consumer" },
      { title: i18n.t("docs.topic.step6"), dom: "py-fullexample" },
    ],
  },
  {
    name: "Go",
    docs: mdx.topicGoDoc,
    steps: [
      { title: i18n.t("docs.topic.go.step1"), dom: "go-initialize-module-depend" },
      { title: i18n.t("docs.topic.step2"), dom: "go-config" },
      { title: i18n.t("docs.topic.step3"), dom: "go-create-consumer" },
      { title: i18n.t("docs.topic.step4"), dom: "go-subscribe-consume" },
      { title: i18n.t("docs.topic.step5"), dom: "go-close-consumer" },
      { title: i18n.t("docs.topic.step6"), dom: "go-fullexample" },
    ],
  },
  {
    name: "Rust",
    docs: mdx.topicRustDoc,
    steps: [
      { title: i18n.t("docs.topic.rust.step1"), dom: "rust-create-project" },
      { title: i18n.t("docs.topic.step2"), dom: "rust-config" },
      { title: i18n.t("docs.topic.step3"), dom: "rust-create-consumer" },
      { title: i18n.t("docs.topic.step4"), dom: "rust-subscribe-consume" },
      { title: i18n.t("docs.topic.step5"), dom: "rust-close-consumer" },
      { title: i18n.t("docs.topic.step6"), dom: "rust-fullexample" },
    ],
  },
  {
    name: 'Java',
    docs: mdx.topicJavaDoc,
    steps: [
      { title: i18n.t('docs.topic.createProject'), dom: 'init' },
      { title: i18n.t('docs.topic.step2'), dom: 'config' },
      { title: i18n.t('docs.topic.step3'), dom: 'create-consumer' },
      { title: i18n.t('docs.topic.step4'), dom: 'subscribe-consume' },
      { title: i18n.t('docs.topic.step5'), dom: 'close-consumer' },
      { title: i18n.t('docs.topic.step6'), dom: 'full-example' }
    ]
  }
];
