# Vue 3 + TypeScript + Vite

This template should help get you started developing with Vue 3 and TypeScript in Vite. The template uses Vue 3 `<script setup>` SFCs, check out the [script setup docs](https://v3.vuejs.org/api/sfc-script-setup.html#sfc-script-setup) to learn more.

Learn more about the recommended Project Setup and IDE Support in the [Vue Docs TypeScript Guide](https://vuejs.org/guide/typescript/overview.html#project-setup).

## Prerequisites

Explorer depends on the following projects:

- [taos-ui](https://github.com/taosdata/taos-ui/tree/datain)
- [@tdengine/websocket](https://github.com/taosdata.com/taos-connector-node/tree/cloud)

They are injected as subtree in the explorer project, so you don't need to clone them separately.

For contributing to these projects, you can clone them separately and link them to the explorer project.

You can also use the following command to pull the latest code of these projects:

```bash
git subtree pull --prefix explorer/taos-ui
git subtree pull --prefix explorer/taos-connector-node
```

## Build

The project is built with Vite, and managed by PNPM.

```bash
pnpm install
pnpm build
```
