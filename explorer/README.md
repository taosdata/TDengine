# Vue 3 + TypeScript + Vite

This template should help get you started developing with Vue 3 and TypeScript in Vite. The template uses Vue 3 `<script setup>` SFCs, check out the [script setup docs](https://v3.vuejs.org/api/sfc-script-setup.html#sfc-script-setup) to learn more.

Learn more about the recommended Project Setup and IDE Support in the [Vue Docs TypeScript Guide](https://vuejs.org/guide/typescript/overview.html#project-setup).

# project link

## link taos-ui

export taos-ui, in the taos-ui root path, execute the following shell:

```bash
pnpm link -g
```

then, in the explorer root path, execute:

```bash
pnpm link taos-ui -g
```

## link @tdengine/websocket

export @tdengine/websocket, in path `taos-connector-node/WebSocket`, execute the following shell:

```bash
pnpm link -g
```

then, in the taos-ui root path, execute:

```bash
pnpm link @tdengine/websocket -g
```
