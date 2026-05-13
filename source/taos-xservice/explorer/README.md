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

## Development with Remote API Server

To develop against a remote explorer API server, configure `.env.dev` to proxy API requests through Vite instead of making direct cross-origin calls. This avoids `SameSite` cookie issues that cause 401 errors on authenticated requests.

1. Edit `.env.dev` and set the API paths to relative, with the remote server as the proxy target:

   ```env
   VITE_APP_EXPLORER_API=/api/-
   VITE_APP_X_API=/api/x
   VITE_DEV_PROXY_TARGET=http://<remote-server>:6060
   ```

2. Start the dev server:

   ```bash
   pnpm dev
   ```

   Vite will proxy all `/api` requests to the remote server. The browser sees same-origin requests, so session cookies are sent correctly.

> **Note:** Do not use absolute URLs (e.g. `http://192.168.x.x:6060/api/-`) for `VITE_APP_EXPLORER_API` / `VITE_APP_X_API` in dev mode — cross-origin POST requests won't carry `SameSite=Lax` cookies, resulting in 401 after login.

## Build

The project is built with Vite, and managed by PNPM.

```bash
pnpm install
pnpm build
```
